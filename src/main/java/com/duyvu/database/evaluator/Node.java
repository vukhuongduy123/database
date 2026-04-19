package com.duyvu.database.evaluator;

public interface Node {
  boolean evaluate(EvaluationContext context);

  OperandNode getOperand(String operandName);
}
