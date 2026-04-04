package com.duyvu.database.engine;

import com.duyvu.database.command.CreateTableCommand;
import com.duyvu.database.command.InsertCommand;
import com.duyvu.database.command.SelectCommand;
import com.duyvu.database.result.DeleteResult;
import com.duyvu.database.result.SelectResult;
import com.duyvu.database.schema.Table;

public class DatabaseEngine {
  private final TableCommandHandler tableCommandHandler = new TableCommandHandler();

  private DatabaseEngine() {}

  public void insert(InsertCommand insertCommand) {
    tableCommandHandler.insert(insertCommand);
  }

  public DeleteResult delete(SelectCommand selectCommand) {
    return tableCommandHandler.delete(selectCommand);
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

  public SelectResult select(SelectCommand selectCommand) {
    return tableCommandHandler.select(selectCommand);
  }
}
