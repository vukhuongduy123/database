package com.duyvu.database.command;

import com.duyvu.database.evaluator.Node;
import java.util.List;
import lombok.Builder;

@Builder
public record SelectCommand(
    String tableName, Node whereExpression, long limit, List<String> columnNames) {}
