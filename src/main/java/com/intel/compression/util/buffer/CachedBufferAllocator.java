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

package com.intel.compression.util.buffer;

import java.lang.ref.SoftReference;
import java.util.*;
import java.nio.ByteBuffer;

import com.intel.compression.jni.IntelCompressionCodecJNI;

/**
 * Cached buffer
 */
public class CachedBufferAllocator implements BufferAllocator 
{
  private static BufferAllocatorFactory factory = new BufferAllocatorFactory()
  {
    @Override
    public BufferAllocator getBufferAllocator(int bufferSize)
    {
      return CachedBufferAllocator.getAllocator(bufferSize);
    }
  };

  public static void setBufferAllocatorFactory(BufferAllocatorFactory factory)
  {
    assert (factory != null);
    CachedBufferAllocator.factory = factory;
  }

  public static BufferAllocatorFactory getBufferAllocatorFactory()
  {
    return factory;
  }

  /**
   * Use SoftReference so that having this queueTable does not prevent the GC of CachedBufferAllocator instances
   */
  private static final Map<Integer, SoftReference<CachedBufferAllocator>> queueTable = new HashMap<Integer, SoftReference<CachedBufferAllocator>>();

  private final int bufferSize;
  private final Deque<ByteBuffer> directByteBufferQueue;
  private final Deque<byte[]> byteArrayQueue;

  public CachedBufferAllocator(int bufferSize)
  {
    this.bufferSize = bufferSize;
    this.byteArrayQueue = new ArrayDeque<byte[]>();
    this.directByteBufferQueue = new ArrayDeque<ByteBuffer>();
  }

  public static synchronized CachedBufferAllocator getAllocator(int bufferSize)
  {
    CachedBufferAllocator result = null;

    if (queueTable.containsKey(bufferSize)) {
      result = queueTable.get(bufferSize).get();
    }
    if (result == null) {
      result = new CachedBufferAllocator(bufferSize);
      queueTable.put(bufferSize, new SoftReference<CachedBufferAllocator>(result));
    }
    return result;
  }

  /**
   * Allocate a direct byte buffer
   *
   * @param size the size of the direct byte buffer to be allocated
   * @return direct byte buffer, the allocate direct byte buffer
   */
  @Override
  public ByteBuffer allocateDirectByteBuffer(boolean useNativeBuffer, int size, int align)
  {
    synchronized (this) {
      if (directByteBufferQueue.isEmpty()) {
        if (useNativeBuffer)
        {
          return (ByteBuffer)IntelCompressionCodecJNI.allocNativeBuffer(size, align);
        }
        else
        {
          return ByteBuffer.allocateDirect(size);
        }
      }
      else {
        return directByteBufferQueue.pollFirst();
      }
    }
  }

  /**
   * Release a direct byte buffer
   *
   * @param buffer the direct byte buffer to be released
   * @return none
   */
  @Override
  public void releaseDirectByteBuffer(ByteBuffer buffer)
  {
    synchronized (this) {
      directByteBufferQueue.addLast(buffer);
    }
  }

  /**
   * Allocate a byte buffer
   *
   * @param size the size of the byte buffer to be allocated
   * @return byte buffer, the allocate byte buffer
   */
  @Override
  public byte[] allocateByteArray(int size)
  {
    synchronized (this) {
      if (byteArrayQueue.isEmpty()) {
        return new byte[size];
      }
      else {
        return byteArrayQueue.pollFirst();
      }
    }
  }

  /**
   * Release a byte buffer
   *
   * @param buffer the byte buffer to be released
   * @return none
   */
  @Override
  public void releaseByteArray(byte[] buffer)
  {
    synchronized (this) {
      byteArrayQueue.addLast(buffer);
    }
  }
}
