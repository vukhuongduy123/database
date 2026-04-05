package com.duyvu.database.schema;

public interface TypeLengthValue {

  Type getType();

  default int getLength() {
    return getValue().length;
  }

  byte[] getValue();
}
