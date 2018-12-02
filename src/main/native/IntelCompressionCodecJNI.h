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

#ifndef _INTEL_COMPRESSION_CODEC_JNI_H_
#define  _INTEL_COMPRESSION_CODEC_JNI_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

typedef enum intel_codec
{
    INTEL_CODEC_RAW        = 0,
    INTEL_CODEC_LZ4_IPP    = 1,
    INTEL_CODEC_LZ4_HC_IPP = 2,
    INTEL_CODEC_ZLIB_IPP   = 3,
    INTEL_CODEC_IGZIP      = 4,
    INTEL_CODEC_ZSTD       = 5,
    INTEL_CODEC_ZLIB_FPGA  = 6,
} intel_codec_t;

typedef struct intel_codec_context
{
    int codec;
    int level;
} intel_codec_context_t;

typedef struct intel_codec_header
{
    uint32_t magic;
    uint32_t codec;
    uint32_t compressed_size;
    uint32_t uncompressed_size;
} intel_codec_header_t;

typedef int32_t (*init_func)(void);

typedef int32_t (*compress_func)(intel_codec_context_t *context,
    const uint8_t *src, uint32_t srcLen, uint8_t *dst, uint32_t *dstLen);

typedef int32_t (*decompress_func)(intel_codec_context_t *context,
    const uint8_t *src, uint32_t srcLen, uint8_t *dst, uint32_t *dstLen);

typedef char* (*get_library_name_func)();

#ifdef __cplusplus
}
#endif

#endif /* _INTEL_COMPRESSION_CODEC_JNI_H_ */
