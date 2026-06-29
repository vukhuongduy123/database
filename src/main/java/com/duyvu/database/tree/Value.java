package com.duyvu.database.tree;

import com.duyvu.database.schema.Type;
import com.duyvu.database.schema.TypeLengthValue;

public record Value(byte[] val) implements TypeLengthValue {
  @Override
  public byte[] getValue() {
    return val;
  }

  @Override
  public Type getType() {
    return Type.VALUE;
  }
}
