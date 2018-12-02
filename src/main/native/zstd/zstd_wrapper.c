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

typedef size_t (*dlsym_ZSTD_compress)(void* dst, size_t dstCapacity,
        const void* src, size_t srcSize,
        int compressionLevel);
typedef size_t (*dlsym_ZSTD_decompress)(void* dst, size_t dstCapacity,
        const void* src, size_t compressedSize);
typedef unsigned (*dlsym_ZSTD_isError)(size_t code);

typedef struct zstd_wrapper_context {                                                                                                     int magic;
    dlsym_ZSTD_compress compress;
    dlsym_ZSTD_decompress decompress;
    dlsym_ZSTD_isError isError;
} zstd_wrapper_context_t;

zstd_wrapper_context_t g_zstd_wrapper_context;

#define ZSTD_LIBRARY_NAME "libzstd.so"

int32_t zstd_wrapper_init(void)
{
    zstd_wrapper_context_t *zstd_wrapper_context = &g_zstd_wrapper_context;
    void *lib = dlopen(ZSTD_LIBRARY_NAME, RTLD_LAZY | RTLD_GLOBAL);
    if (!lib)
    {
        fprintf(stderr, "Cannot load %s due to %s\n", ZSTD_LIBRARY_NAME, dlerror());
        return -1;
    }

    dlerror(); // Clear any existing error

    zstd_wrapper_context->compress = dlsym(lib, "ZSTD_compress");
    if (zstd_wrapper_context->compress == NULL)
    {
        fprintf(stderr, "Failed to load ZSTD_compress\n");
        return -1;
    }

    zstd_wrapper_context->decompress = dlsym(lib, "ZSTD_decompress");
    if (zstd_wrapper_context->decompress == NULL)
    {
        fprintf(stderr, "Failed to load ZSTD_decompress\n");
        return -1;
    }

    zstd_wrapper_context->isError = dlsym(lib, "ZSTD_isError");
    if (zstd_wrapper_context->isError == NULL)
    {
        fprintf(stderr, "Failed to load ZSTD_isError\n");
        return -1;
    }

    zstd_wrapper_context->magic = ('Z' | ('S' << 8) | ('T' << 16) | ('D' << 24));

    return 0;
}

int32_t zstd_wrapper_compress(intel_codec_context_t *context,
        const uint8_t *src, uint32_t srcLen, uint8_t *dst, uint32_t *dstLen)
{
    zstd_wrapper_context_t *zstd_wrapper_context = &g_zstd_wrapper_context;
    intel_codec_header_t *header = (intel_codec_header_t *)dst;
    header->magic = zstd_wrapper_context->magic;
    header->codec = INTEL_CODEC_ZSTD;
    header->uncompressed_size = srcLen;

    int dstCapacity = *dstLen;
    uint8_t *compressed_buffer = dst + sizeof(intel_codec_header_t);
    int compressed_size = zstd_wrapper_context->compress(
            compressed_buffer, dstCapacity, src, srcLen, context->level);
    *dstLen = header->compressed_size = compressed_size + sizeof(intel_codec_header_t);
    return zstd_wrapper_context->isError(compressed_size) ? -1 : 0;
}

int32_t zstd_wrapper_decompress(intel_codec_context_t *context,
    const uint8_t *src, uint32_t srcLen, uint8_t *dst, uint32_t *dstLen)
{
    zstd_wrapper_context_t *zstd_wrapper_context = &g_zstd_wrapper_context;
    intel_codec_header_t *header = (intel_codec_header_t *)src;
    const uint8_t *compressed_buffer = src + sizeof(intel_codec_header_t);
    if (header->magic != zstd_wrapper_context->magic)
    {
        fprintf(stderr, "Wrong magic header for ZSTD codec\n");
        return -1;
    }
    int dstCapacity = *dstLen;
    int uncompressed_size = zstd_wrapper_context->decompress(
            dst, dstCapacity,
            compressed_buffer, header->compressed_size - sizeof(intel_codec_header_t));
    if (uncompressed_size != header->uncompressed_size)
    {
        fprintf(stderr, "Wrong uncompressed size for ZSTD codec, should %d but after decompress is %d\n",
            header->uncompressed_size, uncompressed_size);
        return -1;
    }
    *dstLen = uncompressed_size;
    return zstd_wrapper_context->isError(uncompressed_size) ? -1 : 0;
}

char *zstd_wrapper_get_library_name()
{
    return ZSTD_LIBRARY_NAME;
}
