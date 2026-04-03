package com.duyvu.database.engine;

import com.duyvu.database.command.CreateTableCommand;
import com.duyvu.database.schema.Table;
import com.duyvu.database.utils.EnvironmentUtils;
import com.duyvu.database.utils.PathUtils;

import java.nio.file.Path;
import java.nio.file.Paths;

public class DatabaseEngine {
	private DatabaseEngine() {
	}

	private static final class InstanceHolder {
		private static final DatabaseEngine instance = new DatabaseEngine();
	}

	public static DatabaseEngine getInstance() {
		return InstanceHolder.instance;
	}

	public Table createTable(CreateTableCommand createTableCommand) {
		String databasePath = EnvironmentUtils.getDatabasePath();
		Path tablePath = Paths.get(databasePath, createTableCommand.getName());

		Table table = new Table(createTableCommand.getHeader(), tablePath);

		if (PathUtils.isFile(tablePath)) {
			throw new IllegalArgumentException("Table path is not a file");
		}

		try {
			PathUtils.createFileIfNotExists(tablePath);
		} catch (Exception e) {
			throw new RuntimeException("Failed to create table file", e);
		}
		
		return table;
	}
}
