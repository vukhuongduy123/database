package com.duyvu.database.engine;

import com.duyvu.database.command.*;
import com.duyvu.database.result.*;
import com.duyvu.database.schema.Table;

public final class DatabaseEngine {
  private final TableCommandHandler tableCommandHandler = new TableCommandHandler();

  private DatabaseEngine() {}

  private static final class InstanceHolder {
    private static final DatabaseEngine instance = new DatabaseEngine();
  }

  public static DatabaseEngine getInstance() {
    return InstanceHolder.instance;
  }

  public Table readTable(String tableName) {
    return tableCommandHandler.getTable(tableName);
  }

  public QueryResult execute(Command command) {
    return switch (command) {
      case InsertCommand insertCommand -> {
        tableCommandHandler.insert(insertCommand);
        yield new InsertResult();
      }
      case DeleteCommand deleteCommand -> tableCommandHandler.delete(deleteCommand);
      case CreateTableCommand createTableCommand ->
          new CreateTableResult(tableCommandHandler.createTable(createTableCommand));
      case SelectCommand selectCommand -> tableCommandHandler.select(selectCommand);
      case UpdateCommand updateCommand -> tableCommandHandler.update(updateCommand);
      default -> throw new IllegalStateException("Unexpected value: " + command);
    };
  }
}
