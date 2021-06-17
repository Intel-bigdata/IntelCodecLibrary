package com.intel.compression.util;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class Platform {

  private static final Unsafe _UNSAFE;

  private static final int majorVersion =
          Integer.parseInt(System.getProperty("java.version").split("\\D+")[0]);
  private static final boolean unaligned;

  /**
   * @return true when running JVM is having sun's Unsafe package available in it and underlying
   *         system having unaligned-access capability.
   */
  public static boolean unaligned() {
    return unaligned;
  }

  public static void copyMemory(
          Object src, long srcOffset, Object dst, long dstOffset, long length) {
    // Check if dstOffset is before or after srcOffset to determine if we should copy
    // forward or backwards. This is necessary in case src and dst overlap.
    if (dstOffset < srcOffset) {
      while (length > 0) {
        long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
        _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
        length -= size;
        srcOffset += size;
        dstOffset += size;
      }
    } else {
      srcOffset += length;
      dstOffset += length;
      while (length > 0) {
        long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
        srcOffset -= size;
        dstOffset -= size;
        _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
        length -= size;
      }

    }
  }

  /**
   * Raises an exception bypassing compiler checks for checked exceptions.
   */
  public static void throwException(Throwable t) {
    _UNSAFE.throwException(t);
  }

  /**
   * Limits the number of bytes to copy per {@link Unsafe#copyMemory(long, long, long)} to
   * allow safepoint polling during a large copy.
   */
  private static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

  static {
    sun.misc.Unsafe unsafe;
    try {
      Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      unsafe = (sun.misc.Unsafe) unsafeField.get(null);
    } catch (Throwable cause) {
      unsafe = null;
    }
    _UNSAFE = unsafe;
  }

  // This requires `majorVersion` and `_UNSAFE`.
  static {
    boolean _unaligned;
    String arch = System.getProperty("os.arch", "");
    if (arch.equals("ppc64le") || arch.equals("ppc64") || arch.equals("s390x")) {
      // Since java.nio.Bits.unaligned() doesn't return true on ppc (See JDK-8165231), but
      // ppc64 and ppc64le support it
      _unaligned = true;
    } else {
      try {
        Class<?> bitsClass =
                Class.forName("java.nio.Bits", false, ClassLoader.getSystemClassLoader());
        if (_UNSAFE != null && majorVersion >= 9) {
          // Java 9/10 and 11/12 have different field names.
          Field unalignedField =
                  bitsClass.getDeclaredField(majorVersion >= 11 ? "UNALIGNED" : "unaligned");
          _unaligned = _UNSAFE.getBoolean(
                  _UNSAFE.staticFieldBase(unalignedField), _UNSAFE.staticFieldOffset(unalignedField));
        } else {
          Method unalignedMethod = bitsClass.getDeclaredMethod("unaligned");
          unalignedMethod.setAccessible(true);
          _unaligned = Boolean.TRUE.equals(unalignedMethod.invoke(null));
        }
      } catch (Throwable t) {
        // We at least know x86 and x64 support unaligned access.
        //noinspection DynamicRegexReplaceableByCompiledPattern
        _unaligned = arch.matches("^(i[3-6]86|x86(_64)?|x64|amd64|aarch64)$");
      }
    }
    unaligned = _unaligned;
  }
}
