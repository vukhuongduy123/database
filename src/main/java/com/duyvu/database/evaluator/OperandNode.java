package com.duyvu.database.evaluator;

import com.duyvu.database.schema.RecordValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

@RequiredArgsConstructor
@Log4j2
public class OperandNode implements Node {
  public enum Operand {
    EQ,
    NEQ,
    GT,
    LT,
    GTE,
    LTE
  }

  private final String variable;
  private final Operand operand;
  private final RecordValue recordValue;

  @Override
  public boolean evaluate(EvaluationContext context) {
    Object compareValue = context.get(variable);

    if (compareValue == null) {
      return false;
    }

    return compare(recordValue, compareValue);
  }

  private boolean compare(RecordValue recordValue, Object compareValue) {
    if (recordValue.getOriginalValue().getClass() != compareValue.getClass()) {
      log.debug(
          "Operator mismatch: {} and {}",
          recordValue.getOriginalValue().getClass(),
          compareValue.getClass());
      return false;
    }

    return switch (recordValue.getType()) {
      case STRING -> {
        String v = (String) recordValue.getOriginalValue();
        String c = (String) compareValue;

        yield compare(operand, v, c);
      }
      case INT -> {
        Integer v = (int) recordValue.getOriginalValue();
        Integer c = (int) compareValue;

        yield compare(operand, v, c);
      }

      case LONG -> {
        Long v = (long) recordValue.getOriginalValue();
        Long c = (long) compareValue;

        yield compare(operand, v, c);
      }
      case DOUBLE -> {
        Double v = (double) recordValue.getOriginalValue();
        Double c = (double) compareValue;

        yield compare(operand, v, c);
      }
      case FLOAT -> {
        Float v = (float) recordValue.getOriginalValue();
        Float c = (float) compareValue;

        yield compare(operand, v, c);
      }
      default -> throw new IllegalArgumentException("Unsupported type: " + recordValue.getType());
    };
  }

  private <T> boolean compare(Operand operand, T c, Comparable<T> v) {
    return switch (operand) {
      case EQ -> v.equals(c);
      case NEQ -> !v.equals(c);
      case GT -> v.compareTo(c) > 0;
      case LT -> v.compareTo(c) < 0;
      case GTE -> v.compareTo(c) >= 0;
      case LTE -> v.compareTo(c) <= 0;
    };
  }
}
