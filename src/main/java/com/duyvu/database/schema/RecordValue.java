package com.duyvu.database.schema;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class RecordValue implements TypeLengthValue {
  private final Type type;
  private final byte[] value;
  @Getter
  private final Object originalValue;

  private Type toType(Object o) {
    return switch (o) {
      case String ignored -> Type.STRING;
      case Integer ignored -> Type.INT;
      case Long ignored -> Type.LONG;
      case Double ignored -> Type.DOUBLE;
      case Float ignored -> Type.FLOAT;
      default -> throw new IllegalArgumentException("Unsupported type: " + o.getClass().getName());
    };
  }

  private byte[] toValue(Type type, Object o) {
    return switch (type) {
      case STRING -> ((String) o).getBytes();
      case INT -> ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt((int) o).array();
      case LONG -> ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong((long) o).array();
      case DOUBLE -> ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble((double) o).array();
      case FLOAT -> ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putFloat((float) o).array();
      default -> throw new IllegalArgumentException("Unsupported type: " + type);
    };
  }

  public RecordValue(Object object) {
    this.originalValue = object;
    this.type = toType(object);
    this.value = toValue(this.type, object);
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public byte[] getValue() {
    return value;
  }
}
