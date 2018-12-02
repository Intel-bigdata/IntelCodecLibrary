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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.intel.compression.jni.IntelCompressionCodecJNI;
import com.intel.compression.util.NativeCodeLoader;
import com.intel.compression.util.buffer.*;

public class IntelCompressionCodecCompressor implements Compressor {
  private static final Logger LOG =
    LoggerFactory.getLogger(IntelCompressionCodecCompressor.class.getName());

  private int uncompressedDirectBufferSize;
  private ByteBuffer compressedDirectBuffer = null;
  private final BufferAllocator compressedBufferAllocator;
  private int compressedDirectBufferSize;
  private ByteBuffer uncompressedDirectBuffer = null;
  private final BufferAllocator uncompressedBufferAllocator;
  private int uncompressedBytesInBuffer;
  private byte[] userBuf = null;
  private int userBufOff = 0, userBufLen = 0;
  private boolean finish, finished;

  private long bytesRead = 0L;
  private long bytesWritten = 0L;

  private long context = 0L;

  static {
    if (!NativeCodeLoader.isNativeCodeLoaded()) {
      try {
        LOG.info("try to load native library");
        NativeCodeLoader.load();
      } catch (Throwable t) {
        LOG.error("failed to load native library", t);
      }
    }
  }

  /**
   * Creates a new compressor.
   *
   * @param codec the compression algorithm.
   * @param level the compression codec level.
   * @param directBufferSize size of the direct buffer to be used.
   */
  public IntelCompressionCodecCompressor(String codec, int level,
          int directBufferSize, boolean useNativeBuffer) {
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
    }

    if(compressedDirectBuffer != null) {
      compressedDirectBuffer.clear();
      compressedDirectBuffer.position(compressedDirectBufferSize);
    }

    context = IntelCompressionCodecJNI.createCompressContext(codec, level);
  }

  /**
   * Sets input data for compression.
   * This should be called whenever #needsInput() returns
   * <code>true</code> indicating that more input data is required.
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
    finished = false;

    if (len > uncompressedDirectBuffer.remaining()) {
      // save data; now !needsInput
      userBuf = b;
      userBufOff = off;
      userBufLen = len;
    } else {
      uncompressedDirectBuffer.put(b, off, len);
      uncompressedBytesInBuffer = uncompressedDirectBuffer.position();
    }

    bytesRead += len;
  }

  /**
   * If a write would exceed the capacity of the direct buffers, it is set
   * aside to be loaded by this function while the compressed data are
   * consumed.
   */
  void setInputFromSavedData() {
    if (0 >= userBufLen) {
      return;
    }
    finished = false;

    uncompressedBytesInBuffer = Math.min(userBufLen, uncompressedDirectBufferSize);
    uncompressedDirectBuffer.put(userBuf, userBufOff,
        uncompressedBytesInBuffer);

    // Note how much data is being fed to codec
    userBufOff += uncompressedBytesInBuffer;
    userBufLen -= uncompressedBytesInBuffer;
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
   * #setInput() should be called to provide more input.
   *
   * @return <code>true</code> if the input data buffer is empty and
   *         #setInput() should be called in order to provide more input.
   */
  @Override
  public boolean needsInput() {
    return !(compressedDirectBuffer.remaining() > 0
        || uncompressedDirectBuffer.remaining() == 0 || userBufLen > 0);
  }

  /**
   * When called, indicates that compression should end
   * with the current contents of the input buffer.
   */
  @Override
  public void finish() {
    finish = true;
  }

  /**
   * Returns true if the end of the compressed
   * data output stream has been reached.
   *
   * @return <code>true</code> if the end of the compressed
   *         data output stream has been reached.
   */
  @Override
  public boolean finished() {
    // Check if all uncompressed data has been consumed
    return (finish && finished && compressedDirectBuffer.remaining() == 0);
  }

  /**
   * Fills specified buffer with compressed data. Returns actual number
   * of bytes of compressed data. A return value of 0 indicates that
   * needsInput() should be called in order to determine if more input
   * data is required.
   *
   * @param b   Buffer for the compressed data
   * @param off Start offset of the data
   * @param len Size of the buffer
   * @return The actual number of bytes of compressed data.
   */
  @Override
  public int compress(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    checkContext();

    // Check if there is compressed data
    int n = compressedDirectBuffer.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      compressedDirectBuffer.get(b, off, n);
      bytesWritten += n;
      return n;
    }

    if (0 == uncompressedDirectBuffer.position()) {
      // No compressed data, so we should have !needsInput or !finished
      setInputFromSavedData();
      if (0 == uncompressedDirectBuffer.position()) {
        // Called without data; write nothing
        finished = true;
        return 0;
      }
    }

    // Re-initialize the codec's output direct-buffer
    compressedDirectBuffer.clear();
    n = IntelCompressionCodecJNI.compress(context,
            uncompressedDirectBuffer, 0, uncompressedBytesInBuffer,
            compressedDirectBuffer, 0, compressedDirectBufferSize);
    compressedDirectBuffer.limit(n);
    uncompressedDirectBuffer.clear(); // codec consumes all buffer input
    uncompressedBytesInBuffer = 0;

    // Set 'finished' if codec has consumed all user-data
    if (0 == userBufLen) {
      finished = true;
    }

    // Get atmost 'len' bytes
    n = Math.min(n, len);
    bytesWritten += n;
    compressedDirectBuffer.get(b, off, n);

    return n;
  }

  /**
   * Resets compressor so that a new set of input data can be processed.
   */
  @Override
  public void reset() {
    finish = false;
    finished = false;
    uncompressedDirectBuffer.clear();
    compressedDirectBuffer.clear();
    compressedDirectBuffer.limit(0);
    uncompressedBytesInBuffer = 0;
    userBufOff = userBufLen = 0;
    bytesRead = bytesWritten = 0L;
  }

  /**
   * Prepare the compressor to be used in a new stream with settings defined in
   * the given Configuration
   *
   * @param conf Configuration from which new setting are fetched
   */
  @Override
  public void reinit(Configuration conf) {
    reset();
  }

  /**
   * Return number of bytes given to this compressor since last reset.
   */
  @Override
  public long getBytesRead() {
    return bytesRead;
  }

  /**
   * Return number of bytes consumed by callers of compress since last reset.
   */
  @Override
  public long getBytesWritten() {
    return bytesWritten;
  }

  /**
   * Closes the compressor and discards any unprocessed input.
   */
  @Override
  public void end() {
  }

  private void checkContext() {
    if (context == 0) {
      throw new NullPointerException("Compressor context not initialized");
    }
  }

}
