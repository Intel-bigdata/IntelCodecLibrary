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
import java.nio.ByteBuffer;

import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.intel.compression.jni.IntelCompressionCodecJNI;
import com.intel.compression.util.NativeCodeLoader;
import com.intel.compression.util.buffer.*;

public class IntelCompressionCodecDecompressor implements Decompressor {
  private static final Logger LOG =
    LoggerFactory.getLogger(IntelCompressionCodecDecompressor.class.getName());

  private int compressedDirectBufferSize;
  private ByteBuffer compressedDirectBuffer = null;
  private final BufferAllocator compressedBufferAllocator;
  private int uncompressedDirectBufferSize;
  private ByteBuffer uncompressedDirectBuffer = null;
  private final BufferAllocator uncompressedBufferAllocator;
  private int compressedBytesInBuffer;
  private byte[] userBuf = null;
  private int userBufOff = 0, userBufLen = 0;
  private boolean finished;

  private long context = 0L;

  static {
    if (!NativeCodeLoader.isNativeCodeLoaded()) {
      try {
        LOG.error("try to load native library");
        NativeCodeLoader.load();
      } catch (Throwable t) {
        LOG.error("failed to load native library", t);
      }
    }
  }

  /**
   * Creates a new compressor.
   *
   * @param directBufferSize size of the direct buffer to be used.
   */
  public IntelCompressionCodecDecompressor(int directBufferSize, boolean useNativeBuffer) {
    this.uncompressedDirectBufferSize = directBufferSize;
    this.compressedDirectBufferSize = directBufferSize * 3 / 2;
    this.uncompressedBufferAllocator = CachedBufferAllocator
        .getBufferAllocatorFactory().getBufferAllocator(uncompressedDirectBufferSize);
    this.compressedBufferAllocator = CachedBufferAllocator
        .getBufferAllocatorFactory().getBufferAllocator(compressedDirectBufferSize);
    this.uncompressedDirectBuffer = uncompressedBufferAllocator
        .allocateDirectByteBuffer(useNativeBuffer, uncompressedDirectBufferSize, 64);
    this.compressedDirectBuffer = compressedBufferAllocator
        .allocateDirectByteBuffer(useNativeBuffer, compressedDirectBufferSize, 64);
    if(uncompressedDirectBuffer != null) {
      uncompressedDirectBuffer.clear();
      uncompressedDirectBuffer.position(uncompressedDirectBufferSize);
    }
    if(compressedDirectBuffer != null) {
      compressedDirectBuffer.clear();
    }

    context = IntelCompressionCodecJNI.createDecompressContext();
  }

  /**
   * Sets input data for decompression.
   * This should be called if and only if {@link #needsInput()} returns
   * <code>true</code> indicating that more input data is required.
   * (Both native and non-native versions of various Decompressors require
   * that the data passed in via <code>b[]</code> remain unmodified until
   * the caller is explicitly notified--via {@link #needsInput()}--that the
   * buffer may be safely modified.  With this requirement, an extra
   * buffer-copy can be avoided.)
   *
   * @param b   Input data
   * @param off Start offset
   * @param len Length
   */
  @Override
  public void setInput(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    this.userBuf = b;
    this.userBufOff = off;
    this.userBufLen = len;

    setInputFromSavedData();

    // Reinitialize codec's output direct-buffer
    uncompressedDirectBuffer.limit(uncompressedDirectBufferSize);
    uncompressedDirectBuffer.position(uncompressedDirectBufferSize);
  }

  /**
   * If a write would exceed the capacity of the direct buffers, it is set
   * aside to be loaded by this function while the compressed data are
   * consumed.
   */
  void setInputFromSavedData() {
    compressedBytesInBuffer = Math.min(userBufLen, compressedDirectBufferSize);

    // Reinitialize codec's input direct buffer
    compressedDirectBuffer.rewind();
    compressedDirectBuffer.put(userBuf, userBufOff,
        compressedBytesInBuffer);

    // Note how much data is being fed to codec
    userBufOff += compressedBytesInBuffer;
    userBufLen -= compressedBytesInBuffer;
  }

  /**
   * Does nothing.
   */
  @Override
  public void setDictionary(byte[] b, int off, int len) {
    // do nothing
  }

  /**
   * Returns true if the input data buffer is empty and
   * {@link #setInput(byte[], int, int)} should be called to
   * provide more input.
   *
   * @return <code>true</code> if the input data buffer is empty and
   *         {@link #setInput(byte[], int, int)} should be called in
   *         order to provide more input.
   */
  @Override
  public boolean needsInput() {
    // Consume remaining compressed data?
    if (uncompressedDirectBuffer.remaining() > 0) {
      return false;
    }

    // Check if codec has consumed all input
    if (compressedBytesInBuffer <= 0) {
      // Check if we have consumed all user-input
      if (userBufLen <= 0) {
        return true;
      } else {
        setInputFromSavedData();
      }
    }

    return false;
  }

  /**
   * Returns <code>false</code>.
   *
   * @return <code>false</code>.
   */
  @Override
  public boolean needsDictionary() {
    return false;
  }

  /**
   * Returns true if the end of the decompressed
   * data output stream has been reached.
   *
   * @return <code>true</code> if the end of the decompressed
   *         data output stream has been reached.
   */
  @Override
  public boolean finished() {
    return (finished && uncompressedDirectBuffer.remaining() == 0);
  }

  /**
   * Fills specified buffer with uncompressed data. Returns actual number
   * of bytes of uncompressed data. A return value of 0 indicates that
   * {@link #needsInput()} should be called in order to determine if more
   * input data is required.
   *
   * @param b   Buffer for the compressed data
   * @param off Start offset of the data
   * @param len Size of the buffer
   * @return The actual number of bytes of compressed data.
   * @throws IOException
   */
  @Override
  public int decompress(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    int n = 0;

    // Check if there is uncompressed data
    n = uncompressedDirectBuffer.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      uncompressedDirectBuffer.get(b, off, n);
      return n;
    }
    if (compressedBytesInBuffer > 0) {
      // Re-initialize the codec's output direct buffer
      uncompressedDirectBuffer.rewind();
      uncompressedDirectBuffer.limit(uncompressedDirectBufferSize);

      // Decompress data
      n = IntelCompressionCodecJNI.decompress(context,
            compressedDirectBuffer, 0, compressedBytesInBuffer,
            uncompressedDirectBuffer, 0, uncompressedDirectBufferSize);
      uncompressedDirectBuffer.limit(n);
      compressedBytesInBuffer = 0;

      if (userBufLen <= 0) {
        finished = true;
      }

      // Get atmost 'len' bytes
      n = Math.min(n, len);
      uncompressedDirectBuffer.get(b, off, n);
    }

    return n;
  }

  /**
   * Returns <code>0</code>.
   *
   * @return <code>0</code>.
   */
  @Override
  public int getRemaining() {
    // Never use this function in BlockDecompressorStream.
    return 0;
  }

  @Override
  public void reset() {
    finished = false;
    compressedBytesInBuffer = 0;
    uncompressedDirectBuffer.limit(uncompressedDirectBufferSize);
    uncompressedDirectBuffer.position(uncompressedDirectBufferSize);
    userBufOff = userBufLen = 0;
  }

  /**
   * Resets decompressor and input and output buffers so that a new set of
   * input data can be processed.
   */
  @Override
  public void end() {
    // do nothing
  }

  private void checkContext() {
    if (context == 0) {
      throw new NullPointerException("Decompressor context not initialized");
    }
  }
}
