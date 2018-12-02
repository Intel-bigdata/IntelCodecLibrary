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

package com.intel.compression.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.*;

import com.intel.compression.jni.IntelCompressionCodecJNI;
import com.intel.compression.hadoop.conf.IntelCompressionCodecConfigurationKeys;

/**
 * This class creates IntelCompressionCodec compressors/decompressors.
 */
public class IntelCompressionCodec implements Configurable, CompressionCodec {

  Configuration conf;

  /**
   * Set the configuration to be used by this object.
   *
   * @param conf the configuration object.
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Return the configuration used by this object.
   *
   * @return the configuration object used by this objec.
   */
  @Override
  public Configuration getConf() {
    return conf;
  }

  public static String getLibraryName(int codec) {
    return IntelCompressionCodecJNI.getLibraryName(codec);
  }

  /**
   * Create a {@link CompressionOutputStream} that will write to the given
   * {@link OutputStream}.
   *
   * @param out the location for the final output stream
   * @return a stream the user can write uncompressed data to have it compressed
   * @throws IOException
   */
  @Override
  public CompressionOutputStream createOutputStream(OutputStream out)
      throws IOException {
    return createOutputStream(out, createCompressor());
  }

  /**
   * Create a {@link CompressionOutputStream} that will write to the given
   * {@link OutputStream} with the given {@link Compressor}.
   *
   * @param out        the location for the final output stream
   * @param compressor compressor to use
   * @return a stream the user can write uncompressed data to have it compressed
   * @throws IOException
   */
  @Override
  public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor)
      throws IOException {
    int bufferSize = conf.getInt(
        IntelCompressionCodecConfigurationKeys.INTEL_COMPRESSION_CODEC_BUFFER_SIZE_KEY,
        IntelCompressionCodecConfigurationKeys.INTEL_COMPRESSION_CODEC_BUFFER_SIZE_DEFAULT);

    int compressionOverhead = 0; //(bufferSize / 6) + 32;

    return new BlockCompressorStream(out, compressor, bufferSize, compressionOverhead);
  }

  /**
   * Get the type of {@link Compressor} needed by this {@link CompressionCodec}.
   *
   * @return the type of compressor needed by this codec.
   */
  @Override
  public Class<? extends Compressor> getCompressorType() {
    return IntelCompressionCodecCompressor.class;
  }

  /**
   * Create a new {@link Compressor} for use by this {@link CompressionCodec}.
   *
   * @return a new compressor for use by this codec
   */
  @Override
  public Compressor createCompressor() {
    String codec = conf.get(
        IntelCompressionCodecConfigurationKeys.INTEL_COMPRESSION_CODEC_KEY,
        IntelCompressionCodecConfigurationKeys.INTEL_COMPRESSION_CODEC_DEFAULT);
    int level = conf.getInt(
        IntelCompressionCodecConfigurationKeys.INTEL_COMPRESSION_CODEC_LEVEL_KEY + codec,
        IntelCompressionCodecConfigurationKeys.INTEL_COMPRESSION_CODEC_LEVEL_DEFAULT);
    int bufferSize = conf.getInt(
        IntelCompressionCodecConfigurationKeys.INTEL_COMPRESSION_CODEC_BUFFER_SIZE_KEY,
        IntelCompressionCodecConfigurationKeys.INTEL_COMPRESSION_CODEC_BUFFER_SIZE_DEFAULT);
    boolean useNativeBuffer = conf.getBoolean(
        IntelCompressionCodecConfigurationKeys.INTEL_COMPRESSION_CODEC_USE_NATIVE_BUFFER_KEY,
        IntelCompressionCodecConfigurationKeys.INTEL_COMPRESSION_CODEC_USE_NATIVE_BUFFER_DEFAULT);

    return new IntelCompressionCodecCompressor(codec, level, bufferSize, useNativeBuffer);
  }

  /**
   * Create a {@link CompressionInputStream} that will read from the given
   * input stream.
   *
   * @param in the stream to read compressed bytes from
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  @Override
  public CompressionInputStream createInputStream(InputStream in)
      throws IOException {
    return createInputStream(in, createDecompressor());
  }

  /**
   * Create a {@link CompressionInputStream} that will read from the given
   * {@link InputStream} with the given {@link Decompressor}.
   *
   * @param in           the stream to read compressed bytes from
   * @param decompressor decompressor to use
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  @Override
  public CompressionInputStream createInputStream(InputStream in,
      Decompressor decompressor) throws IOException {

    int bufferSize = conf.getInt(
        IntelCompressionCodecConfigurationKeys.INTEL_COMPRESSION_CODEC_BUFFER_SIZE_KEY,
        IntelCompressionCodecConfigurationKeys.INTEL_COMPRESSION_CODEC_BUFFER_SIZE_DEFAULT);

    return new BlockDecompressorStream(in, decompressor, bufferSize);
  }

  /**
   * Get the type of {@link Decompressor} needed by this {@link CompressionCodec}.
   *
   * @return the type of decompressor needed by this codec.
   */
  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return IntelCompressionCodecDecompressor.class;
  }

  /**
   * Create a new {@link Decompressor} for use by this {@link CompressionCodec}.
   *
   * @return a new decompressor for use by this codec
   */
  @Override
  public Decompressor createDecompressor() {
    int bufferSize = conf.getInt(
        IntelCompressionCodecConfigurationKeys.INTEL_COMPRESSION_CODEC_BUFFER_SIZE_KEY,
        IntelCompressionCodecConfigurationKeys.INTEL_COMPRESSION_CODEC_BUFFER_SIZE_DEFAULT);
    boolean useNativeBuffer = conf.getBoolean(
        IntelCompressionCodecConfigurationKeys.INTEL_COMPRESSION_CODEC_USE_NATIVE_BUFFER_KEY,
        IntelCompressionCodecConfigurationKeys.INTEL_COMPRESSION_CODEC_USE_NATIVE_BUFFER_DEFAULT);
    return new IntelCompressionCodecDecompressor(bufferSize, useNativeBuffer);
  }

  /**
   * Get the default filename extension for this kind of compression.
   *
   * @return <code>.ibdc</code>.
   */
  @Override
  public String getDefaultExtension() {
    return ".ibdc";
  }
}
