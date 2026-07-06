package com.duyvu.database.result;

import java.time.Duration;

public record InsertResult(Duration executionTime) implements QueryResult {
  public InsertResult() {
    this(Duration.ZERO);
  }

  public InsertResult withExecutionTime(Duration executionTime) {
    return new InsertResult(executionTime);
  }
}
