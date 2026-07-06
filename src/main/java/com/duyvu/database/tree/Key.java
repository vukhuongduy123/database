package com.duyvu.database.tree;

import static java.util.Arrays.compare;
import static java.util.Arrays.compareUnsigned;

import com.duyvu.database.schema.Type;
import com.duyvu.database.schema.TypeLengthValue;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.jspecify.annotations.NonNull;

public final class Key implements TypeLengthValue, Comparable<Key> {
  private final byte[] dataTypeAndValue;

  public Key(Type dataType, byte[] value) {
    this.dataTypeAndValue = new byte[value.length + 1];
    this.dataTypeAndValue[0] = dataType.getCode();
    System.arraycopy(value, 0, this.dataTypeAndValue, 1, value.length);
  }

  public Key(byte[] dataTypeAndValue) {
    this.dataTypeAndValue = dataTypeAndValue;
  }

  @Override
  public Type getType() {
    return Type.KEY;
  }

  @Override
  public byte[] getValue() {
    return dataTypeAndValue;
  }

  @Override
  public int compareTo(@NonNull Key o) {
    if (dataTypeAndValue[0] != o.dataTypeAndValue[0]) {
      throw new IllegalArgumentException("Cannot compare keys of different types");
    }
    Type type = Type.fromCode(dataTypeAndValue[0]);
    return switch (type) {
      case STRING -> compareUnsigned(this.dataTypeAndValue, o.dataTypeAndValue);
      case DOUBLE ->
          Double.compare(
              ByteBuffer.wrap(this.dataTypeAndValue, 1, this.dataTypeAndValue.length - 1)
                  .order(ByteOrder.BIG_ENDIAN)
                  .getDouble(),
              ByteBuffer.wrap(o.dataTypeAndValue, 1, o.dataTypeAndValue.length - 1)
                  .order(ByteOrder.BIG_ENDIAN)
                  .getDouble());
      case FLOAT ->
          Float.compare(
              ByteBuffer.wrap(this.dataTypeAndValue, 1, this.dataTypeAndValue.length - 1)
                  .order(ByteOrder.BIG_ENDIAN)
                  .getFloat(),
              ByteBuffer.wrap(o.dataTypeAndValue, 1, o.dataTypeAndValue.length - 1)
                  .order(ByteOrder.BIG_ENDIAN)
                  .getFloat());
      case LONG ->
          Long.compare(
              ByteBuffer.wrap(this.dataTypeAndValue, 1, this.dataTypeAndValue.length - 1)
                  .order(ByteOrder.BIG_ENDIAN)
                  .getLong(),
              ByteBuffer.wrap(o.dataTypeAndValue, 1, o.dataTypeAndValue.length - 1)
                  .order(ByteOrder.BIG_ENDIAN)
                  .getLong());
      case INT ->
          Integer.compare(
              ByteBuffer.wrap(this.dataTypeAndValue, 1, this.dataTypeAndValue.length - 1)
                  .order(ByteOrder.BIG_ENDIAN)
                  .getInt(),
              ByteBuffer.wrap(o.dataTypeAndValue, 1, o.dataTypeAndValue.length - 1)
                  .order(ByteOrder.BIG_ENDIAN)
                  .getInt());
      default -> compare(this.dataTypeAndValue, o.dataTypeAndValue);
    };
  }
}
