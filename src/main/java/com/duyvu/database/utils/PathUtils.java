package com.duyvu.database.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class PathUtils {
	private PathUtils() {
		throw new IllegalStateException("Utility class");
	}

	public static boolean isFile(Path path) {
		try {
			return Files.isRegularFile(path);
		} catch (Exception e) {
			return false;
		}
	}

	public static void createFileIfNotExists(Path path) throws IOException {
		if (path.getParent() != null) {
			Files.createDirectories(path.getParent());
		}

		if (!Files.exists(path)) {
			Files.createFile(path);
		}
	}

	public static String getFileNameWithoutExtension(Path path) {
		String fileName = path.getFileName().toString();
		int dotIndex = fileName.lastIndexOf('.');
		return dotIndex > 0 ? fileName.substring(0, dotIndex) : fileName;
	}
}
