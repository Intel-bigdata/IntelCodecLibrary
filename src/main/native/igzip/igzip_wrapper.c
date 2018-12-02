/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>

#include "IntelCompressionCodecJNI.h"
#include "igzip_lib.h"

/* 0 <= level <= 1 */
typedef void (*dlsym_isal_deflate_stateless_init)(struct isal_zstream *stream);
typedef int (*dlsym_isal_deflate_stateless)(struct isal_zstream *stream);
typedef void (*dlsym_isal_inflate_init)(struct inflate_state *state);
typedef int (*dlsym_isal_inflate_stateless)(struct inflate_state *state);

typedef struct igzip_wrapper_context {                                                                                                     int magic;
    dlsym_isal_deflate_stateless_init isal_deflate_stateless_init_func;
    dlsym_isal_deflate_stateless isal_deflate_stateless_func;
    dlsym_isal_inflate_init isal_inflate_init_func;
    dlsym_isal_inflate_stateless isal_inflate_stateless_func;
} igzip_wrapper_context_t;

igzip_wrapper_context_t g_igzip_wrapper_context;

#define IGZIP_LIBRARY_NAME "libisal.so"

int32_t igzip_wrapper_init(void)
{
    igzip_wrapper_context_t *igzip_wrapper_context = &g_igzip_wrapper_context;
    void *lib = dlopen(IGZIP_LIBRARY_NAME, RTLD_LAZY | RTLD_GLOBAL);
    if (!lib)
    {
        fprintf(stderr, "Cannot load %s due to %s\n", IGZIP_LIBRARY_NAME, dlerror());
        return -1;
    }

    dlerror(); // Clear any existing error

    igzip_wrapper_context->isal_deflate_stateless_init_func = dlsym(lib,
            "isal_deflate_stateless_init");
    if (igzip_wrapper_context->isal_deflate_stateless_init_func == NULL)
    {
        fprintf(stderr, "Failed to load dlsym_isal_deflate_stateless_init\n");
        return -1;
    }

    igzip_wrapper_context->isal_deflate_stateless_func = dlsym(lib,
            "isal_deflate_stateless");
    if (igzip_wrapper_context->isal_deflate_stateless_func == NULL)
    {
        fprintf(stderr, "Failed to load isal_deflate_stateless\n");
        return -1;
    }

    igzip_wrapper_context->isal_inflate_init_func = dlsym(lib,
            "isal_inflate_init");
    if (igzip_wrapper_context->isal_inflate_init_func == NULL)
    {
        fprintf(stderr, "Failed to load isal_inflate_init\n");
        return -1;
    }

    igzip_wrapper_context->isal_inflate_stateless_func = dlsym(lib,
            "isal_inflate_stateless");
    if (igzip_wrapper_context->isal_inflate_stateless_func == NULL)
    {
        fprintf(stderr, "Failed to load isal_inflate_stateless\n");
        return -1;
    }

    igzip_wrapper_context->magic = ('I' | ('S' << 8) | ('A' << 16) | ('L' << 24));

    return 0;
}

int32_t igzip_wrapper_compress(intel_codec_context_t *context,
    const uint8_t *src, uint32_t srcLen, uint8_t *dst, uint32_t *dstLen)
{
    igzip_wrapper_context_t *igzip_wrapper_context = &g_igzip_wrapper_context;
    intel_codec_header_t *header = (intel_codec_header_t *)dst;
    header->magic = igzip_wrapper_context->magic;
    header->codec = INTEL_CODEC_IGZIP;
    header->uncompressed_size = srcLen;

    struct isal_zstream stream;
    uint32_t level_buf_size = ISAL_DEF_LVL1_EXTRA_LARGE;
    uint8_t *level_buf = NULL;

    igzip_wrapper_context->isal_deflate_stateless_init_func(&stream);
    stream.end_of_stream = 1;   /* Do the entire file at once */
    stream.flush = NO_FLUSH;
    stream.next_in = (uint8_t *)src;
    stream.avail_in = srcLen;
    stream.next_out = (uint8_t *)(dst + sizeof(intel_codec_header_t));
    stream.avail_out = *dstLen - sizeof(intel_codec_header_t);
    stream.level = context->level;
    if (context->level >= 1)
    {
        level_buf = malloc(level_buf_size);
        if (level_buf == NULL)
        {
            fprintf(stderr, "IGZIP compress: Failed to alloc deflate level buffer\n");
        }
    }
    stream.level_buf = level_buf;
    stream.level_buf_size = level_buf_size;
    int ret = igzip_wrapper_context->isal_deflate_stateless_func(&stream);
    if (ret != COMP_OK)
    {
        if (ret == STATELESS_OVERFLOW)
        {
            fprintf(stderr, "IGZIP deflate: output buffer will not fit output\n");
            return -1;
        }
        else if (ret == INVALID_FLUSH)
        {
            fprintf(stderr, "IGZIP deflate: an invalid FLUSH is selected\n");
            return -1;
        }
        else if (ret == ISAL_INVALID_LEVEL)
        {
            fprintf(stderr, "IGZIP deflate: an invalid compression level is selected\n");
            return -1;
        }
    }
    if (stream.avail_in != 0)
    {
        fprintf(stderr, "IGZIP: Could not compress all of inbuf\n");
        return -1;
    }

    *dstLen = header->compressed_size = stream.total_out + sizeof(intel_codec_header_t);

    if (level_buf != NULL)
    {
        free(level_buf);
    }

    return 0;
}

int32_t igzip_wrapper_decompress(intel_codec_context_t *context,
    uint8_t *src, uint32_t srcLen, uint8_t *dst, uint32_t *dstLen)
{
    igzip_wrapper_context_t *igzip_wrapper_context = &g_igzip_wrapper_context;
    intel_codec_header_t *header = (intel_codec_header_t *)src;
    if (header->magic != g_igzip_wrapper_context.magic)
    {
        fprintf(stderr, "Wrong magic header for IGZIP codec.\n");
        return -1;
    }

    struct inflate_state state;
    igzip_wrapper_context->isal_inflate_init_func(&state);
    state.next_in = (uint8_t *)(src + sizeof(intel_codec_header_t));
    state.avail_in = header->compressed_size - sizeof(intel_codec_header_t);
    state.next_out = (uint8_t *)dst;
    state.avail_out = *dstLen;

    int ret = igzip_wrapper_context->isal_inflate_stateless_func(&state);
    if (ret != ISAL_DECOMP_OK)
    {
        if (ret == ISAL_END_INPUT)
        {
            fprintf(stderr, "isal_inflate_stateless: End of input reached\n");
        }
        else if (ret == ISAL_OUT_OVERFLOW)
        {
            fprintf(stderr, "isal_inflate_stateless: output buffer ran out of space\n");
        }
        else if (ret == ISAL_INVALID_BLOCK)
        {
            fprintf(stderr, "isal_inflate_stateless: Invalid deflate block found\n");
        }
        else if (ret == ISAL_INVALID_SYMBOL)
        {
            fprintf(stderr, "isal_inflate_stateless: Invalid deflate symbol found\n");
        }
        else if (ret == ISAL_INVALID_LOOKBACK)
        {
            fprintf(stderr, "isal_inflate_stateless: Invalid lookback distance found\n");
        }
        return -1;
    }

    if (state.total_out!= header->uncompressed_size)
    {
        fprintf(stderr, "Wrong uncompressed size for igzip codec, should %d but after decompress is %d\n",
            header->uncompressed_size, state.total_out);
        return -1;
    }

    *dstLen = state.total_out;
    return 0;
}

char *igzip_wrapper_get_library_name()
{
    return IGZIP_LIBRARY_NAME;
}
