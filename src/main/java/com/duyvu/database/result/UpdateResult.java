package com.duyvu.database.result;

import java.time.Duration;

public record UpdateResult(String tableName, int affectedRows, Duration executionTime)
    implements QueryResult {
  public UpdateResult(String tableName, int affectedRows) {
    this(tableName, affectedRows, Duration.ZERO);
  }

  public UpdateResult withExecutionTime(Duration executionTime) {
    return new UpdateResult(this.tableName(), this.affectedRows(), executionTime);
  }
}
