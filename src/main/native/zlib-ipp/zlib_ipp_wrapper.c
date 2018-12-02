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
#include "zlib.h"

/* -1 <= level <= 9, default is 6, IPP also support level -2 */
typedef size_t (*dlsym_compress2)(uint8_t *dest, size_t *destLen,
        const uint8_t *source, size_t sourceLen,
        int level);
typedef size_t (*dlsym_uncompress)(uint8_t *dest, size_t *destLen,
        const uint8_t *source, size_t sourceLen);

typedef struct zlib_ipp_wrapper_context {                                                                                                     int magic;
    dlsym_compress2 compress;
    dlsym_uncompress decompress;
} zlib_ipp_wrapper_context_t;

zlib_ipp_wrapper_context_t g_zlib_ipp_wrapper_context;

#define ZLIB_IPP_LIBRARY_NAME "libz.so"

int32_t zlib_ipp_wrapper_init(void)
{
    zlib_ipp_wrapper_context_t *zlib_ipp_wrapper_context = &g_zlib_ipp_wrapper_context;
    void *lib = dlopen(ZLIB_IPP_LIBRARY_NAME, RTLD_LAZY | RTLD_GLOBAL);
    if (!lib)
    {
        fprintf(stderr, "Cannot load %s due to %s\n", ZLIB_IPP_LIBRARY_NAME, dlerror());
        return -1;
    }

    dlerror(); // Clear any existing error

    zlib_ipp_wrapper_context->compress = dlsym(lib, "compress2");
    if (zlib_ipp_wrapper_context->compress == NULL)
    {
        fprintf(stderr, "ZLIB-IPP: Failed to load compress2\n");
        return -1;
    }

    zlib_ipp_wrapper_context->decompress = dlsym(lib, "uncompress");
    if (zlib_ipp_wrapper_context->decompress == NULL)
    {
        fprintf(stderr, "ZLIB-IPP: Failed to load uncompress\n");
        return -1;
    }

    zlib_ipp_wrapper_context->magic = ('Z' | ('L' << 8) | ('B' << 16) | ('I' << 24));

    return 0;
}

int32_t zlib_ipp_wrapper_compress(intel_codec_context_t *context,
        const uint8_t *src, uint32_t srcLen, uint8_t *dst, uint32_t *dstLen)
{
    zlib_ipp_wrapper_context_t *zlib_ipp_wrapper_context = &g_zlib_ipp_wrapper_context;
    intel_codec_header_t *header = (intel_codec_header_t *)dst;
    header->magic = zlib_ipp_wrapper_context->magic;
    header->codec = INTEL_CODEC_ZLIB_IPP;
    header->uncompressed_size = srcLen;

    uint8_t *compressed_buffer = dst + sizeof(intel_codec_header_t);
    size_t compressed_size = *dstLen - sizeof(intel_codec_header_t);
    int ret = zlib_ipp_wrapper_context->compress(
            compressed_buffer, &compressed_size,
            src, (size_t)srcLen,
            context->level);
    if (ret != Z_OK)
    {
        if (ret == Z_MEM_ERROR)
        {
            fprintf(stderr, "ZLIB-IPP: not enough memory for compress\n");
        }
        else if (ret == Z_BUF_ERROR)
        {
            fprintf(stderr, "ZLIB-IPP: not enough room in the output buffer for compress\n");
        }
        return -1;
    }

    *dstLen = header->compressed_size = compressed_size + sizeof(intel_codec_header_t);
    return 0;
}

int32_t zlib_ipp_wrapper_decompress(intel_codec_context_t *context,
        const uint8_t *src, uint32_t srcLen, uint8_t *dst, uint32_t *dstLen)
{
    zlib_ipp_wrapper_context_t *zlib_ipp_wrapper_context = &g_zlib_ipp_wrapper_context;
    intel_codec_header_t *header = (intel_codec_header_t *)src;
    uint8_t *compressed_buffer = src + sizeof(intel_codec_header_t);
    if (header->magic != zlib_ipp_wrapper_context->magic)
    {
        fprintf(stderr, "Wrong magic header for ZLIB IPP codec\n");
        return -1;
    }
    size_t uncompressed_size = *dstLen;
    int ret = zlib_ipp_wrapper_context->decompress(
            dst, &uncompressed_size,
            compressed_buffer,
            (size_t)(header->compressed_size - sizeof(intel_codec_header_t)));
    if (ret != Z_OK)
    {
        if (ret == Z_MEM_ERROR)
        {
            fprintf(stderr, "ZLIB-IPP: not enough memory for uncomprss\n");
        }
        else if (ret == Z_BUF_ERROR)
        {
            fprintf(stderr, "ZLIB-IPP: not enough room in the output buffer for uncompress\n");
        }
        else if (ret == Z_DATA_ERROR)
        {
            fprintf(stderr, "ZLIB-IPP: input data was corrupted or incomplete for uncompress\n");
        }
        return -1;
    }
    if (uncompressed_size != header->uncompressed_size)
    {
        fprintf(stderr, "Wrong uncompressed size after decompress for ZLIB IPP codec\n");
        return -1;
    }
    *dstLen = uncompressed_size;
    return 0;
}

char *zlib_ipp_wrapper_get_library_name()
{
    return ZLIB_IPP_LIBRARY_NAME;
}
