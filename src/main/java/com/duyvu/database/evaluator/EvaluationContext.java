package com.duyvu.database.evaluator;

import java.util.Map;

public class EvaluationContext {

  private final Map<String, Object> variables;

  public EvaluationContext(Map<String, Object> variables) {
    this.variables = variables;
  }

  public Object get(String name) {
    return variables.get(name);
  }
}
