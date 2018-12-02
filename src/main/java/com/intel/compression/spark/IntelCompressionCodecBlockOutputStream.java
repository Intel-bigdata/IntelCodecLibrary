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

package com.intel.compression.spark;

import java.io.IOException;
import java.io.OutputStream;
import java.io.FilterOutputStream;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.intel.compression.util.buffer.*;
import com.intel.compression.jni.IntelCompressionCodecJNI;

/**
 * Streaming IntelCompressionCodec.
 * <p>
 * This class compresses data into fixed-size blocks of compressed data.
 * @see IntelCompressionCodecBlockInputStream
 */
public final class IntelCompressionCodecBlockOutputStream extends FilterOutputStream {

  private static final Logger LOG =
      LoggerFactory.getLogger(IntelCompressionCodecBlockOutputStream.class);

  private long context;
  private String codec;
  private int level;
  private int compressedBlockSize;
  private int uncompressedBlockSize;
  private BufferAllocator compressedBufferAllocator;
  private BufferAllocator uncompressedBufferAllocator;
  private ByteBuffer compressedBuffer;
  private ByteBuffer uncompressedBuffer;
  private boolean closed;
  private int uncompressedBufferPosition;
  private byte[] tempBuffer;
  private final BufferAllocator tempBufferAllocator;
  static final int HEADER_LENGTH = 4;         // decompressed length

  /**
   * Create a new {@link OutputStream} with configurable codec, level and block size. Large
   * blocks require more memory at compression and decompression time but
   * should improve the compression ratio.
   *
   * @param out         the {@link OutputStream} to feed
   * @param codec       the compression codec used to compress data
   * @param level       the compression codec level
   * @param blockSize   the maximum number of bytes to try to compress at once,
   *                    must be >= 32 K
   */
  public IntelCompressionCodecBlockOutputStream(OutputStream out, String codec,
          int level, int blockSize, boolean useNativeBuffer) {
    super(out);
    this.codec = codec;
    this.level = level;
    this.uncompressedBlockSize = blockSize;
    this.compressedBlockSize = blockSize * 3 / 2;
    this.uncompressedBufferAllocator = CachedBufferAllocator.
            getBufferAllocatorFactory().getBufferAllocator(uncompressedBlockSize);
    this.compressedBufferAllocator = CachedBufferAllocator.
            getBufferAllocatorFactory().getBufferAllocator(compressedBlockSize);
    this.uncompressedBuffer = uncompressedBufferAllocator.
            allocateDirectByteBuffer(useNativeBuffer, uncompressedBlockSize, 64);
    this.compressedBuffer = compressedBufferAllocator.
            allocateDirectByteBuffer(useNativeBuffer, compressedBlockSize, 64);

    if(uncompressedBuffer != null) {
      uncompressedBuffer.clear();
    }

    if(compressedBuffer != null) {
      compressedBuffer.clear();
    }

    uncompressedBufferPosition = 0;
    closed = false;

    tempBufferAllocator = CachedBufferAllocator.getBufferAllocatorFactory().
            getBufferAllocator(compressedBlockSize);
    tempBuffer = tempBufferAllocator.allocateByteArray(compressedBlockSize);

    context = IntelCompressionCodecJNI.createCompressContext(codec, level);
    LOG.debug("Create OutputStream with codec " + codec + ", level " + level);
  }

  private void checkStream() {
    if (context == 0) {
      throw new NullPointerException();
    }
    if (closed) {
      throw new IllegalStateException("This output stream is already closed");
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    checkStream();
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException("BlockOutputStream write requested lenght " + len
          + " from offset " + off + " in buffer of size " + b.length);
    }

    while (uncompressedBufferPosition + len > uncompressedBlockSize) {
      int left = uncompressedBlockSize - uncompressedBufferPosition;
      uncompressedBuffer.put(b, off, left);
      uncompressedBufferPosition = uncompressedBlockSize;
      compressBufferedData();
      off += left;
      len -= left;
    }
    uncompressedBuffer.put(b, off, len);
    uncompressedBufferPosition += len;
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(int b) throws IOException {
    byte[] oneByte = new byte[1];
    oneByte[0] = (byte) b;
    write(oneByte, 0, 1);
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    try {
      finish();
      out.close();
    }
    finally {
      closed = true;
      uncompressedBufferAllocator.releaseDirectByteBuffer(uncompressedBuffer);
      compressedBufferAllocator.releaseDirectByteBuffer(compressedBuffer);
      tempBufferAllocator.releaseByteArray(tempBuffer);
      tempBuffer = null;
      out = null;
      IntelCompressionCodecJNI.destroyContext(context);
      context = 0;
    }
    LOG.debug("Close OutputStream with codec " + codec + ", level " + level);
  }

  private void compressBufferedData() throws IOException {
    if (uncompressedBufferPosition == 0) {
      return;
    }
    int compressedLength = IntelCompressionCodecJNI.compress(context,
        uncompressedBuffer, 0, uncompressedBufferPosition,
        compressedBuffer, 0, compressedBlockSize);
    writeIntLE(compressedLength, tempBuffer, 0);
    compressedBuffer.position(0);
    compressedBuffer.limit(compressedLength);
    int totalWritten = 0;
    int off = 4;
    while (totalWritten < compressedLength) {
      int bytesToWrite = Math.min((compressedLength - totalWritten), tempBuffer.length - off);
      compressedBuffer.get(tempBuffer, off, bytesToWrite);
      out.write(tempBuffer, 0, bytesToWrite + off);
      totalWritten += bytesToWrite;
      off = 0;
    }
    uncompressedBuffer.clear();
    compressedBuffer.clear();
    uncompressedBufferPosition = 0;
  }

  public void finish() throws IOException {
    checkStream();
    compressBufferedData();
    out.flush();
  }

  private static void writeIntLE(int i, byte [] buf, int off) {
    buf[off] = (byte)i;
    buf[off + 1]= (byte)(i >>> 8);
    buf[off + 2]= (byte)(i >>> 16);
    buf[off + 3]= (byte)(i >>> 24);
    //buf.putInt(off, i);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(out=" + out
        + ", codec=" + codec + ", level=" + level
        + ", blockSize=" + uncompressedBlockSize + ")";
  }
}
