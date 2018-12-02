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

#define _GNU_SOURCE
#include <jni.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <stdlib.h>
#include <string.h>

#include "IntelCompressionCodecJNI.h"
#include "lz4_ipp_wrapper.h"
#include "zlib_ipp_wrapper.h"
#include "igzip_wrapper.h"
#include "zstd_wrapper.h"

static int32_t raw_wrapper_compress(intel_codec_context_t *context,
    const uint8_t *src, uint32_t srcLen, uint8_t *dst, uint32_t *dstLen);
static int32_t raw_wrapper_decompress(intel_codec_context_t *context,
    const uint8_t *src, uint32_t srcLen, uint8_t *dst, uint32_t *dstLen);
static char *raw_wrapper_get_library_name();

/* A helper macro to 'throw' a java exception. */
#define THROW(env, exception_name, message) \
{ \
    jclass ecls = (*env)->FindClass(env, exception_name); \
    if (ecls) { \
        (*env)->ThrowNew(env, ecls, message); \
        (*env)->DeleteLocalRef(env, ecls); \
    } \
}

typedef struct intel_codec_dest
{
    const char*             name;
    compress_func           compress;
    decompress_func         decompress;
    init_func               init;
    get_library_name_func   get_library_name;
} intel_codec_desc_t;

static intel_codec_desc_t intel_codec_table[] =
{
    {"raw", raw_wrapper_compress, raw_wrapper_decompress, NULL, raw_wrapper_get_library_name},
    {"lz4-ipp", lz4_ipp_wrapper_compress, lz4_ipp_wrapper_decompress, lz4_ipp_wrapper_init, NULL},
    {"lz4-hc-ipp", lz4_ipp_wrapper_compress_hc, lz4_ipp_wrapper_decompress, NULL, NULL},
    {"zlib-ipp", zlib_ipp_wrapper_compress, zlib_ipp_wrapper_decompress, zlib_ipp_wrapper_init, NULL},
    {"igzip", igzip_wrapper_compress, igzip_wrapper_decompress, igzip_wrapper_init, NULL},
    {"zstd", zstd_wrapper_compress, zstd_wrapper_decompress, zstd_wrapper_init, zstd_wrapper_get_library_name},
};

/*
 * Class:     com_intel_compression_jni_IntelCompressionCodecJNI
 * Method:    init
 * Signature: ()V
 */
JNIEXPORT void JNICALL
Java_com_intel_compression_jni_IntelCompressionCodecJNI_init(
        JNIEnv *env, jclass cls)
{
    int i = 0;
    for (i = 0; i < sizeof(intel_codec_table) / sizeof( intel_codec_table[0]); i++)
    {
        if (intel_codec_table[i].init)
        {
            int ret = intel_codec_table[i].init();
            if (ret)
            {
                char msg[128];
                snprintf(msg, 128, "Can't load codec %s's library!", intel_codec_table[i].name);
                THROW(env, "java/lang/UnsatisfiedLinkError", msg);
            }
        }
    }
}

/*
 * Class:     com_intel_compression_jni_IntelCompressionCodecJNI
 * Method:    allocNativeBuffer
 * Signature: (II)Ljava/lang/Object;
 */
JNIEXPORT jobject JNICALL
Java_com_intel_compression_jni_IntelCompressionCodecJNI_allocNativeBuffer(
        JNIEnv *env, jclass cls, jint capacity, jint align)
{
    void *buffer = NULL;
    posix_memalign (&buffer, align, capacity);
    if (buffer != NULL)
    {
        return (*env)->NewDirectByteBuffer(env, buffer, capacity);
    }
    else
    {
        THROW(env, "java/lang/OutOfMemoryError", "Error alloc the native buffer");
        return NULL;
    }
}

/*
 * Class:     com_intel_compression_jni_IntelCompressionCodecJNI
 * Method:    createCompressContext
 * Signature: (Ljava/lang/String;I)J
 */
JNIEXPORT jlong JNICALL
Java_com_intel_compression_jni_IntelCompressionCodecJNI_createCompressContext(
        JNIEnv *env, jclass cls, jstring codec_name_from_java, jint level)
{
    intel_codec_context_t *context = malloc(sizeof(intel_codec_context_t));
    if (context == NULL)
    {
        THROW(env, "java/lang/OutOfMemoryError", "Error alloc the compress context");
        return (jlong)0;
    }

    context->level = level;
    context->codec = 0;

    int i = 0;
    const char *codec_name = (*env)->GetStringUTFChars(env, codec_name_from_java, NULL);
    for (i = 1; i < (sizeof(intel_codec_table) / sizeof(intel_codec_table[0])); i++)
    {
        if (strncmp(codec_name, intel_codec_table[i].name, strlen(intel_codec_table[i].name)) == 0)
        {
            context->codec = i;
            break;
        }
    }

    if (i == (sizeof(intel_codec_table) / sizeof(intel_codec_table[0])))
    {
        if (i > 1)
        {
            context->codec = 1;
            fprintf(stderr, "Can't find codec %s, fallback to codec %s\n", codec_name, intel_codec_table[1].name);
        }
    }

    (*env)->ReleaseStringUTFChars(env, codec_name_from_java, codec_name);

    return (jlong)context;
}

/*
 * Class:     com_intel_compression_jni_IntelCompressionCodecJNI
 * Method:    createDecompressContext
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL
Java_com_intel_compression_jni_IntelCompressionCodecJNI_createDecompressContext(
        JNIEnv *env, jclass cls)
{
    intel_codec_context_t *context = malloc(sizeof(intel_codec_context_t));
    if (context == NULL)
    {
        THROW(env, "java/lang/OutOfMemoryError", "Error alloc the decompress context");
        return (jlong)0;
    }

    return (jlong)context;
}

/*
 * Class:     com_intel_compression_jni_IntelCompressionCodecJNI
 * Method:    destroyContext
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_com_intel_compression_jni_IntelCompressionCodecJNI_destroyContext(
        JNIEnv *env, jclass cls, jlong contextFromJava)
{
    intel_codec_context_t *context = (intel_codec_context_t *)contextFromJava;
    free(context);
}

/*
 * Class:     com_intel_compression_jni_IntelCompressionCodecJNI
 * Method:    compress
 * Signature: (JLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL
Java_com_intel_compression_jni_IntelCompressionCodecJNI_compress(
        JNIEnv *env, jclass cls, jlong contextFromJava,
        jobject srcBuffer, jint srcOff, jint srcLen,
        jobject destBuffer, jint destOff, jint destLen)
{

    uint8_t* in;
    uint8_t* out;
    uint32_t compressed_size = 0;
    intel_codec_context_t *context = (intel_codec_context_t *)contextFromJava;

    in = (uint8_t*)(*env)->GetDirectBufferAddress(env, srcBuffer);
    if (in == NULL)
    {
        THROW(env, "java/lang/OutOfMemoryError", "Can't get compressor input buffer");
    }

    out = (uint8_t*)(*env)->GetDirectBufferAddress(env, destBuffer);
    if (out == NULL)
    {
        THROW(env, "java/lang/OutOfMemoryError", "Can't get compressor output buffer");
    }

    in += srcOff;
    out += destOff;

    if ((context->codec >= 0)
        && (context->codec < (sizeof(intel_codec_table) / sizeof(intel_codec_table[0])))
        && (intel_codec_table[context->codec].compress != NULL))
    {
        compressed_size = destLen;
        int ret = intel_codec_table[context->codec].compress(
            context, in, srcLen, out, &compressed_size);
        if ((ret != 0))
                //|| (compressed_size > srcLen))
        {
            raw_wrapper_compress(context, in, srcLen, out, &compressed_size);
        }
    }
    else
    {
        THROW(env, "java/lang/InternalError", "Unsupport compress codec type.");
    }

    return compressed_size;
}

/*
 * Class:     com_intel_compression_jni_IntelCompressionCodecJNI
 * Method:    decompress
 * Signature: (JLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL
Java_com_intel_compression_jni_IntelCompressionCodecJNI_decompress(
        JNIEnv *env, jclass cls, jlong contextFromJava,
        jobject srcBuffer, jint srcOff, jint srcLen,
        jobject destBuffer, jint destOff, jint destLen)
{

    uint8_t* in;
    uint8_t* out;
    uint32_t uncompressed_size = 0;
    intel_codec_context_t *context = (intel_codec_context_t *)contextFromJava;

    in = (uint8_t*)(*env)->GetDirectBufferAddress(env, srcBuffer);
    if (in == NULL)
    {
        THROW(env, "java/lang/OutOfMemoryError", "Can't get decompressor input buffer");
    }

    out = (uint8_t*)(*env)->GetDirectBufferAddress(env, destBuffer);
    if (out == NULL)
    {
        THROW(env, "java/lang/OutOfMemoryError", "Can't get decompressor output buffer");
    }

    in += srcOff;
    out += destOff;

    intel_codec_header_t *header = (intel_codec_header_t *)in;

    if ((header->codec >= 0)
        && (header->codec < (sizeof(intel_codec_table) / sizeof(intel_codec_table[0])))
        && (intel_codec_table[header->codec].decompress != NULL))
    {
        uncompressed_size = destLen;
        int ret = intel_codec_table[header->codec].decompress(
            context, in, srcLen, out, &uncompressed_size);
        if (ret != 0)
        {
            THROW(env, "java/lang/InternalError", "Could not decompress data.");
        }
    }
    else
    {
        THROW(env, "java/lang/InternalError", "Unsupport decompress codec type.");
    }

    return uncompressed_size;
}

/*
 * Class:     com_intel_compression_jni_IntelCompressionCodecJNI
 * Method:    getLibraryName
 * Signature: (I)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL
Java_com_intel_compression_jni_IntelCompressionCodecJNI_getLibraryName(
        JNIEnv *env, jclass cls, jint codec)
{
    if ((codec >= 0)
        && (codec < (sizeof(intel_codec_table) / sizeof(intel_codec_table[0])))
        && (intel_codec_table[codec].get_library_name != NULL))
    {
        char *name = intel_codec_table[codec].get_library_name();
        return (*env)->NewStringUTF(env, name);
    } else {
        return (*env)->NewStringUTF(env, "Unavailable");
    }
}

static int32_t raw_wrapper_compress(intel_codec_context_t *context,
        const uint8_t *src, uint32_t srcLen, uint8_t *dst, uint32_t *dstLen)
{
    intel_codec_header_t *header = (intel_codec_header_t *)dst;
    header->magic = ('I' | ('R' << 8) | ('A' << 16) | ('W' << 24));
    header->codec = INTEL_CODEC_RAW;
    memcpy(dst + sizeof(intel_codec_header_t), src, srcLen);
    header->uncompressed_size = srcLen;
    *dstLen = header->compressed_size = srcLen + sizeof(intel_codec_header_t);
    return 0;
}

static int32_t raw_wrapper_decompress(intel_codec_context_t *context,
        const uint8_t *src, uint32_t srcLen, uint8_t *dst, uint32_t *dstLen)
{
    intel_codec_header_t *header = (intel_codec_header_t *)src;
    if (header->codec != INTEL_CODEC_RAW)
    {
        fprintf(stderr, "Wrong codec for RAW codec\n");
        return -1;
    }
    if (header->magic != ('I' | ('R' << 8) | ('A' << 16) | ('W' << 24)))
    {
        fprintf(stderr, "Wrong magic for RAW codec\n");
        return -1;
    }
    memcpy(dst, src + sizeof(intel_codec_header_t), header->uncompressed_size);
    *dstLen = header->uncompressed_size;
    return 0;
}

static char *raw_wrapper_get_library_name()
{
    return "memcpy";
}
