package com.duyvu.database.tree;

import com.duyvu.database.schema.Type;
import com.duyvu.database.schema.TypeLengthValue;
import java.nio.ByteBuffer;

public record Value(ByteBuffer val) implements TypeLengthValue {
  @Override
  public byte[] getValue() {
    return val.array();
  }

  @Override
  public Type getType() {
    return Type.VALUE;
  }
}
