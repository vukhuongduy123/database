package com.duyvu.database.schema;

public interface TypeLengthValue {
  int META_DATA_LENGTH = 5;

  Type getType();

  int getLength();

  byte[] getValue();
}
