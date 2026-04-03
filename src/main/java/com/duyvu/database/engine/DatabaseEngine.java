package com.duyvu.database.engine;

import com.duyvu.database.command.CreateTableCommand;
import com.duyvu.database.schema.Table;

public class DatabaseEngine {
	private final TableCommandHandler tableCommandHandler = new TableCommandHandler();
	private DatabaseEngine() {
	}

	private static final class InstanceHolder {
		private static final DatabaseEngine instance = new DatabaseEngine();
	}

	public static DatabaseEngine getInstance() {
		return InstanceHolder.instance;
	}

	public Table createTable(CreateTableCommand createTableCommand) {
		return tableCommandHandler.createTable(createTableCommand);
	}
	
	public Table readTable(String tableName) {
		return tableCommandHandler.getTable(tableName);
	}
}
