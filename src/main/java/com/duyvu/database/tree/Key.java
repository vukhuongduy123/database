package com.duyvu.database.tree;

import static java.util.Arrays.compare;
import static java.util.Arrays.compareUnsigned;

import com.duyvu.database.schema.Type;
import com.duyvu.database.schema.TypeLengthValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.jspecify.annotations.NonNull;

public final class Key implements TypeLengthValue, Comparable<Key> {
  private final Type dataType;
  private final byte[] value;
  private final byte[] dataTypeAndValue;

  public Key(Type dataType, byte[] value) {
    this.dataType = dataType;
    this.value = value;
    this.dataTypeAndValue = new byte[value.length + 1];
    this.dataTypeAndValue[0] = dataType.getCode();
    System.arraycopy(value, 0, this.dataTypeAndValue, 1, value.length);
  }

  public Key(byte[] dataTypeAndValue) {
    this.dataTypeAndValue = dataTypeAndValue;
    this.dataType = Type.fromCode(dataTypeAndValue[0]);
    this.value = new byte[dataTypeAndValue.length - 1];
    System.arraycopy(dataTypeAndValue, 1, this.value, 0, this.value.length);
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
    if (dataType != o.dataType) {
      throw new IllegalArgumentException("Cannot compare keys of different types");
    }
    return switch (dataType) {
      case STRING -> compareUnsigned(this.value, o.value);
      case DOUBLE ->
          Double.compare(
              ByteBuffer.wrap(this.value).order(ByteOrder.BIG_ENDIAN).getDouble(),
              ByteBuffer.wrap(o.value).order(ByteOrder.BIG_ENDIAN).getDouble());
      case FLOAT ->
          Float.compare(
              ByteBuffer.wrap(this.value).order(ByteOrder.BIG_ENDIAN).getFloat(),
              ByteBuffer.wrap(o.value).order(ByteOrder.BIG_ENDIAN).getFloat());
      case LONG ->
          Long.compare(
              ByteBuffer.wrap(this.value).order(ByteOrder.BIG_ENDIAN).getLong(),
              ByteBuffer.wrap(o.value).order(ByteOrder.BIG_ENDIAN).getLong());
      case INT ->
          Integer.compare(
              ByteBuffer.wrap(this.value).order(ByteOrder.BIG_ENDIAN).getInt(),
              ByteBuffer.wrap(o.value).order(ByteOrder.BIG_ENDIAN).getInt());
      default -> compare(this.value, o.value);
    };
  }
}
