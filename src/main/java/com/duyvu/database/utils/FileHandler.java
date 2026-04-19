package com.duyvu.database.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class FileHandler {
  private FileHandler() {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  for (RandomAccessFile raf : fileHandlerMap.values()) {
                    try {
                      raf.getFD().sync(); // flush to disk
                      raf.close();
                    } catch (IOException e) {
                      log.error("Error closing file", e);
                    }
                  }
                }));
  }

  private static final class InstanceHolder {
    private static final FileHandler instance = new FileHandler();
  }

  public static FileHandler getInstance() {
    return FileHandler.InstanceHolder.instance;
  }

  private final Map<Path, RandomAccessFile> fileHandlerMap = new ConcurrentHashMap<>();

  public synchronized void addFileHandler(Path path) {
    RandomAccessFile raf;
    try {
      raf = new RandomAccessFile(path.toFile(), "rw");
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
    fileHandlerMap.put(path, raf);
  }

  public RandomAccessFile getFileHandler(Path path) {
    return fileHandlerMap.get(path);
  }
}
