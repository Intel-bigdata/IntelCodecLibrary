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

package com.intel.compression.hadoop.conf;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeys;

import com.intel.compression.hadoop.*;

@InterfaceAudience.Private
@InterfaceStability.Unstable

public class IntelCompressionCodecConfigurationKeys extends CommonConfigurationKeys {

  /** Intel Compression Codec */
  public static final String INTEL_COMPRESSION_CODEC_KEY =
    "io.compression.codec.intel.codec";

  /** Default value for INTEL_COMPRESSION_CODEC_KEY. */
  public static final String INTEL_COMPRESSION_CODEC_DEFAULT = "lz4-ipp";

  /** Intel Compression Codec compression level. */
  public static final String INTEL_COMPRESSION_CODEC_LEVEL_KEY =
    "io.compression.codec.intel.level.";

  /** Default value for INTEL_COMPRESSION_CODEC_LEVEL_KEY. */
  public static final int INTEL_COMPRESSION_CODEC_LEVEL_DEFAULT = 1;

  /** Intel Compression Codec buffer size. */
  public static final String INTEL_COMPRESSION_CODEC_BUFFER_SIZE_KEY =
    "io.compression.codec.intel.bufferSize";

  /** Default value for INTEL_COMPRESSION_CODEC_BUFFER_SIZE_KEY */
  public static final int
    INTEL_COMPRESSION_CODEC_BUFFER_SIZE_DEFAULT = 1024 * 1024;

  /** Intel Compression Codec buffer size. */
  public static final String INTEL_COMPRESSION_CODEC_USE_NATIVE_BUFFER_KEY =
    "io.compression.codec.intel.useNativeBuffer";

  /** Default value for INTEL_COMPRESSION_CODEC_USE_NATIVE_BUFFER_KEY */
  public static final boolean
    INTEL_COMPRESSION_CODEC_USE_NATIVE_BUFFER_DEFAULT = false;

}

