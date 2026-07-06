package com.duyvu.database.result;

import com.duyvu.database.schema.Table;
import java.time.Duration;

public record CreateTableResult(Table table, Duration executionTime) implements QueryResult {
  public CreateTableResult(Table table) {
    this(table, Duration.ZERO);
  }

  public CreateTableResult withExecutionTime(Duration executionTime) {
    return new CreateTableResult(this.table(), executionTime);
  }
}
