package com.duyvu.database.tree;

import com.duyvu.database.schema.Type;
import com.duyvu.database.schema.TypeLengthValue;

import static java.util.Arrays.compareUnsigned;

public record Key(byte[] val) implements TypeLengthValue, Comparable<Key> {
  @Override
  public Type getType() {
    return Type.KEY;
  }

  @Override
  public byte[] getValue() {
    return val;
  }

  @Override
  public int compareTo(Key o) {
    return compareUnsigned(val, o.val);
  }
}
