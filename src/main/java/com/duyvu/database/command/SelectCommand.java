package com.duyvu.database.command;

import com.duyvu.database.evaluator.Node;

public record SelectCommand(String tableName, Node whereExpression) {}
