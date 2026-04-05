package com.duyvu.database.utils;

public final class Constants {
  private Constants() {
    throw new IllegalStateException("Utility class");
  }

  public static final int META_DATA_LENGTH = 5;
  public static final long UNLIMITED = Long.MAX_VALUE;
  public static final long UNKNOWN_OFFSET = -1;
}
