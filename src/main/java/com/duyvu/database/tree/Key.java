package com.duyvu.database.tree;

import com.duyvu.database.schema.Type;
import com.duyvu.database.schema.TypeLengthValue;

import java.nio.ByteBuffer;

public record Key(ByteBuffer val) implements TypeLengthValue, Comparable<Key> {
  @Override
  public Type getType() {
    return Type.KEY;
  }

  @Override
  public byte[] getValue() {
    return val.array();
  }

  @Override
  public int compareTo(Key o) {
    return this.val.compareTo(o.val);
  }
}
