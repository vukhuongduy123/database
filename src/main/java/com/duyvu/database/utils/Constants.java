package com.duyvu.database.utils;

public final class Constants {
  private Constants() {
    throw new IllegalStateException("Utility class");
  }

  public static final int PAGE_SIZE = 4096 * 4;
  public static final long UNKNOWN_OFFSET = -1;
  public static final int META_DATA_LENGTH = 5;
}
