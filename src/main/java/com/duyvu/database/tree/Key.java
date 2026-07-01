package com.duyvu.database.tree;

import static java.util.Arrays.compare;
import static java.util.Arrays.compareUnsigned;

import com.duyvu.database.schema.Type;
import com.duyvu.database.schema.TypeLengthValue;
import com.duyvu.database.utils.CollectionUtils;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.jspecify.annotations.NonNull;

public record Key(Type dataType, byte[] value) implements TypeLengthValue, Comparable<Key> {

  public static Key of(byte[] dataTypeAndValue) {
    byte dataType = dataTypeAndValue[0];
    byte[] value = new byte[dataTypeAndValue.length - 1];
    System.arraycopy(dataTypeAndValue, 1, value, 0, value.length);
    return new Key(Type.fromCode(dataType), value);
  }

  @Override
  public Type getType() {
    return Type.KEY;
  }

  @Override
  public byte[] getValue() {
    return CollectionUtils.concat(dataType.getCode(), value);
  }

  @Override
  public int compareTo(@NonNull Key o) {
    if (dataType != o.dataType) {
      throw new IllegalArgumentException("Cannot compare keys of different types");
    }
    return switch (dataType) {
      case STRING -> compareUnsigned(getValue(), o.getValue());
      case DOUBLE ->
          Double.compare(
              ByteBuffer.wrap(getValue()).order(ByteOrder.BIG_ENDIAN).getDouble(),
              ByteBuffer.wrap(o.getValue()).order(ByteOrder.BIG_ENDIAN).getDouble());
      case FLOAT ->
          Float.compare(
              ByteBuffer.wrap(getValue()).order(ByteOrder.BIG_ENDIAN).getFloat(),
              ByteBuffer.wrap(o.getValue()).order(ByteOrder.BIG_ENDIAN).getFloat());
      case LONG ->
          Long.compare(
              ByteBuffer.wrap(getValue()).order(ByteOrder.BIG_ENDIAN).getLong(),
              ByteBuffer.wrap(o.getValue()).order(ByteOrder.BIG_ENDIAN).getLong());
      case INT ->
          Integer.compare(
              ByteBuffer.wrap(getValue()).order(ByteOrder.BIG_ENDIAN).getInt(),
              ByteBuffer.wrap(o.getValue()).order(ByteOrder.BIG_ENDIAN).getInt());
      default -> compare(getValue(), o.getValue());
    };
  }
}
