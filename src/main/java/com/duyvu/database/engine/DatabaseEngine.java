package com.duyvu.database.engine;

import com.duyvu.database.command.*;
import com.duyvu.database.result.*;
import com.duyvu.database.schema.Table;
import java.time.Duration;
import java.time.Instant;

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
    Instant start = Instant.now();
    return switch (command) {
      case InsertCommand insertCommand -> {
        tableCommandHandler.insert(insertCommand);
        Instant end = Instant.now();
        yield new InsertResult().withExecutionTime(Duration.between(start, end));
      }
      case DeleteCommand deleteCommand -> {
        DeleteResult result = tableCommandHandler.delete(deleteCommand);
        Instant end = Instant.now();
        yield result.withExecutionTime(Duration.between(start, end));
      }
      case CreateTableCommand createTableCommand -> {
        CreateTableResult result =
            new CreateTableResult(tableCommandHandler.createTable(createTableCommand));
        Instant end = Instant.now();
        yield result.withExecutionTime(Duration.between(start, end));
      }
      case SelectCommand selectCommand -> {
        SelectResult result = tableCommandHandler.select(selectCommand);
        Instant end = Instant.now();
        yield result.withExecutionTime(Duration.between(start, end));
      }
      case UpdateCommand updateCommand -> {
        UpdateResult result = tableCommandHandler.update(updateCommand);
        Instant end = Instant.now();
        yield result.withExecutionTime(Duration.between(start, end));
      }
      default -> throw new IllegalStateException("Unexpected value: " + command);
    };
  }
}
