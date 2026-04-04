package com.duyvu.database.evaluator;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class OperatorNode implements Node {
  public enum Operator {
    AND,
    OR,
    NOT
  }

  private final Operator operator;
  private final Node left;
  private final Node right;

  @Override
  public boolean evaluate(EvaluationContext context) {
    return switch (operator) {
      case AND -> left.evaluate(context) && right.evaluate(context);
      case OR -> left.evaluate(context) || right.evaluate(context);
      case NOT -> !left.evaluate(context);
    };
  }
}
