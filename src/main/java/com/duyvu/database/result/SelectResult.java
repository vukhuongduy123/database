package com.duyvu.database.result;

import com.duyvu.database.schema.Row;
import java.time.Duration;
import java.util.List;

public record SelectResult(String tableName, List<Row> rows, Duration executionTime)
    implements QueryResult {
  public SelectResult(String tableName, List<Row> rows) {
    this(tableName, rows, Duration.ZERO);
  }

  public SelectResult withExecutionTime(Duration executionTime) {
    return new SelectResult(this.tableName(), this.rows(), executionTime);
  }
}
