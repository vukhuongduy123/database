package com.duyvu.database.result;

import java.time.Duration;

public record DeleteResult(String tableName, int affectedRows, Duration executionTime)
    implements QueryResult {
  public DeleteResult(String tableName, int affectedRows) {
    this(tableName, affectedRows, Duration.ZERO);
  }

  public DeleteResult withExecutionTime(Duration executionTime) {
    return new DeleteResult(this.tableName(), this.affectedRows(), executionTime);
  }
}
