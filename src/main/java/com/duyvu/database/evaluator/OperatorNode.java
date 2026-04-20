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

  @Override
  public OperandNode getOperand(String operandName) {
    if (left != null) {
      OperandNode found = left.getOperand(operandName);
      if (found != null) {
        return found;
      }
    }

    if (right != null) {
      return right.getOperand(operandName);
    }

    return null;
  }

  @Override
  public boolean containsOperator(Operator operator) {
    if (this.operator == operator) {
      return true;
    }

    return (left != null && left.containsOperator(operator))
        || (right != null && right.containsOperator(operator));
  }
}
