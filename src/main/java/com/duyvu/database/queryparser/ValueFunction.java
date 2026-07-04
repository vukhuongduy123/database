package com.duyvu.database.queryparser;

import java.util.Optional;
import java.util.function.Function;

enum ValueFunction {
  LONG("long", Long::parseLong),
  INT("int", Integer::parseInt),
  DOUBLE("double", Double::parseDouble),
  FLOAT("float", Float::parseFloat),
  STRING("string", String::valueOf);

  private final String name;
  private final Function<String, Object> converter;

  ValueFunction(String name, Function<String, Object> converter) {
    this.name = name;
    this.converter = converter;
  }

  @Override
  public String toString() {
    return name;
  }

  public static Optional<ValueFunction> fromName(String name) {
    for (ValueFunction valueFunction : values()) {
      if (valueFunction.name.equalsIgnoreCase(name)) {
        return Optional.of(valueFunction);
      }
    }
    return Optional.empty();
  }

  public Object convert(String value) {
    return converter.apply(value);
  }
}
