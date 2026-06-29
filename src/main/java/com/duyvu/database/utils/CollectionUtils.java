package com.duyvu.database.utils;

public final class CollectionUtils {
  private CollectionUtils() {
    throw new IllegalStateException("Utility class");
  }

  public static byte[] concat(byte a, byte[] b) {
    byte[] result = new byte[1 + b.length];
    result[0] = a;
    System.arraycopy(b, 0, result, 1, b.length);
    return result;
  }
}
