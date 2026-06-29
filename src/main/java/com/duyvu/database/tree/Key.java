package com.duyvu.database.tree;

import static java.util.Arrays.compare;

import com.duyvu.database.schema.Type;
import com.duyvu.database.schema.TypeLengthValue;

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
    return compare(val, o.val);
  }
}
