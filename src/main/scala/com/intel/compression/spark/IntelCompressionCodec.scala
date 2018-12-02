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

package com.intel.compression.spark

import java.io._

import com.intel.compression.spark._

import org.apache.spark.io._
import org.apache.spark.SparkConf

class IntelCompressionCodec(conf: SparkConf) extends CompressionCodec {

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    /**
     *  @param codec the algorithm used for compression
     *  @param level the level for compression
     *  @param bufferSize the size of the buffer used for compression
     *  @param useNativeBuffer whether to enable alloc native buffer in jni
     */
    val codec = conf.get("spark.io.compression.codec.intel.codec", "lz4-ipp")
    val level = conf.getInt("spark.io.compression.codec.intel.level." + codec, 1)
    val bufferSize = conf.getSizeAsBytes("spark.io.compression.codec.intel.blockSize",
        "1024k").toInt
    val useNativeBuffer = conf.getBoolean("spark.io.compression.codec.intel.useNativeBuffer",
        false)
    new IntelCompressionCodecBlockOutputStream(s, codec, level, bufferSize, useNativeBuffer)
  }

  override def compressedInputStream(s: InputStream): InputStream = {
    /**
     *  @param bufferSize the size of the buffer used for compression
     *  @param useNativeBuffer whether to enable alloc native buffer in jni
     */
    val bufferSize = conf.getSizeAsBytes("spark.io.compression.codec.intel.blockSize",
        "1024k").toInt
    val useNativeBuffer = conf.getBoolean("spark.io.compression.codec.intel.useNativeBuffer",
        false)
    new IntelCompressionCodecBlockInputStream(s, bufferSize, useNativeBuffer)
  }
}
