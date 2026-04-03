package com.duyvu.database.schema;

public interface TypeLengthValue {
  int META_DATA_LENGTH = 5;

  Type getType();

  default int getLength() {
    return getValue().length;
  }

  byte[] getValue();
}
