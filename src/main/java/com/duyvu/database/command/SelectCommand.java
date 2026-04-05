package com.duyvu.database.command;

import static com.duyvu.database.utils.Constants.UNLIMITED;

import com.duyvu.database.evaluator.Node;

public record SelectCommand(String tableName, Node whereExpression, long limit) {
  public SelectCommand(String tableName, Node whereExpression) {
    this(tableName, whereExpression, UNLIMITED);
  }
}
