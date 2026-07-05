package com.duyvu.database.command;

import com.duyvu.database.evaluator.Node;

public record DeleteCommand(String tableName, Node whereExpression) implements Command {}
