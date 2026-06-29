package com.duyvu.database.tree;

import static java.util.Arrays.compareUnsigned;

import com.duyvu.database.schema.Type;
import com.duyvu.database.schema.TypeLengthValue;
import org.jspecify.annotations.NonNull;

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
  public int compareTo(@NonNull Key o) {
    return compareUnsigned(getValue(), o.getValue());
  }
}
