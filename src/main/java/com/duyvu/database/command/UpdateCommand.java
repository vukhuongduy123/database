package com.duyvu.database.command;

import com.duyvu.database.evaluator.Node;
import java.util.Map;

public record UpdateCommand(
    String tableName, Node whereExpression, Map<String, Object> newValues) {}
