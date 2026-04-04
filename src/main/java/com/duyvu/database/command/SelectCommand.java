package com.duyvu.database.command;

import com.duyvu.database.evaluator.Node;

public record SelectCommand(String tableName, Node whereExpression, long limit) {
  public static final long UNLIMITED = Long.MAX_VALUE;
  public SelectCommand(String tableName, Node whereExpression) {
    this(tableName, whereExpression, UNLIMITED);
  }
}
