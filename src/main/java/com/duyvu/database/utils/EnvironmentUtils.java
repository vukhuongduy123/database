package com.duyvu.database.utils;

public class EnvironmentUtils {
  private EnvironmentUtils() {
    throw new IllegalStateException("Utility class");
  }

  public static final String DATABASE_PATH = "database.path";
  public static final String DEFAULT_DATABASE_PATH = "./data";

  public static String getDatabasePath() {
    return System.getProperty(DATABASE_PATH, DEFAULT_DATABASE_PATH);
  }
}
