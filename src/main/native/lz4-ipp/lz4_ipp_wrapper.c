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

/* 1 <= acceleration <= 99, but IPP only support 1 */
typedef size_t (*dlsym_LZ4_IPP_compress)(const uint8_t* src, uint8_t* dst,
        int srcSize, int dstCapacity, int acceleration);

/* 1 <= level <= 12, default is 6, IPP doesn't support LZ4 HC mode */
typedef size_t (*dlsym_LZ4_IPP_compress_hc)(const uint8_t* src, uint8_t* dst,
        int srcSize, int dstCapacity, int compressionLevel);

typedef size_t (*dlsym_LZ4_IPP_decompress)(const uint8_t* src, uint8_t* dst,
        int compressedSize, int dstCapacity);

typedef struct lz4_ipp_wrapper_context {                                                                                                     int magic;
    dlsym_LZ4_IPP_compress compress;
    dlsym_LZ4_IPP_compress_hc compress_hc;
    dlsym_LZ4_IPP_decompress decompress;
} lz4_ipp_wrapper_context_t;

lz4_ipp_wrapper_context_t g_lz4_ipp_wrapper_context;

#define LZ4_IPP_LIBRARY_NAME "liblz4.so"

int32_t lz4_ipp_wrapper_init(void)
{
    lz4_ipp_wrapper_context_t *lz4_ipp_wrapper_context = &g_lz4_ipp_wrapper_context;
    void *lib = dlopen(LZ4_IPP_LIBRARY_NAME, RTLD_LAZY | RTLD_GLOBAL);
    if (!lib)
    {
        fprintf(stderr, "Cannot load %s due to %s\n", LZ4_IPP_LIBRARY_NAME, dlerror());
        return -1;
    }

    dlerror(); // Clear any existing error

    lz4_ipp_wrapper_context->compress = dlsym(lib, "LZ4_compress_fast");
    if (lz4_ipp_wrapper_context->compress == NULL)
    {
        fprintf(stderr, "Failed to load LZ4_compress_fast\n");
        return -1;
    }

    lz4_ipp_wrapper_context->compress_hc = dlsym(lib, "LZ4_compress_HC");
    if (lz4_ipp_wrapper_context->compress_hc == NULL)
    {
        fprintf(stderr, "Failed to load LZ4_compress_HC\n");
        return -1;
    }

    lz4_ipp_wrapper_context->decompress = dlsym(lib, "LZ4_decompress_safe");
    if (lz4_ipp_wrapper_context->decompress == NULL)
    {
        fprintf(stderr, "Failed to load LZ4_decompress_safe\n");
        return -1;
    }

    lz4_ipp_wrapper_context->magic = ('L' | ('Z' << 8) | ('4' << 16) | ('I' << 24));

    return 0;
}

int32_t lz4_ipp_wrapper_compress(intel_codec_context_t *context,
    const uint8_t *src, uint32_t srcLen, uint8_t *dst, uint32_t *dstLen)
{
    lz4_ipp_wrapper_context_t *lz4_ipp_wrapper_context = &g_lz4_ipp_wrapper_context;
    intel_codec_header_t *header = (intel_codec_header_t *)dst;
    header->magic = lz4_ipp_wrapper_context->magic;
    header->codec = INTEL_CODEC_LZ4_IPP;
    header->uncompressed_size = srcLen;

    int compressed_size;
    uint8_t *compressed_buffer = dst + sizeof(intel_codec_header_t);
    compressed_size = lz4_ipp_wrapper_context->compress(
            src, compressed_buffer, srcLen, *dstLen,
            context->level);

    if (compressed_size == 0)
    {
        return -1;
    }

    *dstLen = header->compressed_size = compressed_size + sizeof(intel_codec_header_t);
    return 0;
}

int32_t lz4_ipp_wrapper_compress_hc(intel_codec_context_t *context,
    const uint8_t *src, uint32_t srcLen, uint8_t *dst, uint32_t *dstLen)
{
    lz4_ipp_wrapper_context_t *lz4_ipp_wrapper_context = &g_lz4_ipp_wrapper_context;
    intel_codec_header_t *header = (intel_codec_header_t *)dst;
    header->magic = lz4_ipp_wrapper_context->magic;
    header->codec = INTEL_CODEC_LZ4_HC_IPP;
    header->uncompressed_size = srcLen;

    int compressed_size;
    uint8_t *compressed_buffer = dst + sizeof(intel_codec_header_t);
    compressed_size = lz4_ipp_wrapper_context->compress_hc(
            src, compressed_buffer, srcLen, *dstLen,
            context->level);

    if (compressed_size == 0)
    {
        return -1;
    }

    *dstLen = header->compressed_size = compressed_size + sizeof(intel_codec_header_t);
    return 0;
}

int32_t lz4_ipp_wrapper_decompress(intel_codec_context_t *context,
    uint8_t *src, uint32_t srcLen, uint8_t *dst, uint32_t *dstLen)
{
    lz4_ipp_wrapper_context_t *lz4_ipp_wrapper_context = &g_lz4_ipp_wrapper_context;
    intel_codec_header_t *header = (intel_codec_header_t *)src;
    uint8_t *compressed_buffer = src + sizeof(intel_codec_header_t);
    if (header->magic != lz4_ipp_wrapper_context->magic)
    {
        fprintf(stderr, "Wrong magic header for LZ4 IPP codec\n");
        return -1;
    }
    int dstCapacity = *dstLen;
    int uncompressed_size = lz4_ipp_wrapper_context->decompress(
        compressed_buffer, dst,
        header->compressed_size - sizeof(intel_codec_header_t), dstCapacity);
    if (uncompressed_size != header->uncompressed_size)
    {
        fprintf(stderr, "Wrong uncompressed size for LZ4 IPP codec, should %d but after decompress is %d\n", header->uncompressed_size, uncompressed_size);
        return -1;
    }
    *dstLen = uncompressed_size;
    return 0;
}

char *lz4_ipp_wrapper_get_library_name()
{
    return LZ4_IPP_LIBRARY_NAME;
}
