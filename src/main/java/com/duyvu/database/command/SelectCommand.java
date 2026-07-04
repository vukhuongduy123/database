package com.duyvu.database.command;

import static com.duyvu.database.utils.Constants.UNLIMITED;

import com.duyvu.database.evaluator.Node;
import java.util.List;
import lombok.Builder;

@Builder
public record SelectCommand(
    String tableName, Node whereExpression, long limit, List<String> columnNames) {
  public static class SelectCommandBuilder {
    SelectCommandBuilder() {
      limit = UNLIMITED;
    }
  }
}
