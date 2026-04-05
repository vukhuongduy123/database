package com.duyvu.database.schema;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import lombok.Data;

public record ColumnDefinition(
    ColumnName columnName, ColumnType columnType, ColumnAttribute columnAttribute)
    implements TypeLengthValue {

  @Override
  public Type getType() {
    return Type.COLUMN_DEFINITION;
  }

  @Override
  public int getLength() {
    return getValue().length;
  }

  @Override
  public byte[] getValue() {
    ByteBuffer buffer =
        ByteBuffer.allocate(
                columnName.getLength()
                    + columnType.getLength()
                    + columnAttribute.getLength()
                    + META_DATA_LENGTH * 3)
            .order(ByteOrder.BIG_ENDIAN);
    buffer.put(columnName.getType().getCode());
    buffer.putInt(columnName.getLength());
    buffer.put(columnName.getValue());

    buffer.put(columnType.getType().getCode());
    buffer.putInt(columnType.getLength());
    buffer.put(columnType.getValue());

    buffer.put(columnAttribute.getType().getCode());
    buffer.putInt(columnAttribute.getLength());
    buffer.put(columnAttribute.getValue());
    return buffer.array();
  }

  @Data
  public static class ColumnName implements TypeLengthValue {
    private String name;

    public ColumnName(String name) {
      this.name = name;
    }

    @Override
    public Type getType() {
      return Type.STRING;
    }

    @Override
    public int getLength() {
      return getValue().length;
    }

    @Override
    public byte[] getValue() {
      return name.getBytes();
    }
  }

  @Data
  public static class ColumnType implements TypeLengthValue {
    private byte code;

    public ColumnType(Type type) {
      this.code = type.getCode();
    }

    @Override
    public Type getType() {
      return Type.BYTE;
    }

    @Override
    public int getLength() {
      return getValue().length;
    }

    @Override
    public byte[] getValue() {
      return new byte[] {code};
    }
  }

  @Data
  public static class ColumnAttribute implements TypeLengthValue {
    private BitSet attributes;
    public static final byte NULLABLE = 0;
    public static final byte PRIMARY_KEY = 1;
    public static final byte UNIQUE = 2;

    public ColumnAttribute(byte[] attributes) {
      this.attributes = new BitSet(32);
      for (byte attribute : attributes) {
        this.attributes.set(attribute);
      }
    }

    public ColumnAttribute(int value) {
      attributes = new BitSet(32);
      for (int i = 0; i < 32; i++) {
        if ((value & (1 << i)) != 0) {
          attributes.set(i);
        }
      }
    }

    @Override
    public Type getType() {
      return Type.INT;
    }

    @Override
    public int getLength() {
      return getValue().length;
    }

    @Override
    public byte[] getValue() {
      ByteBuffer buffer = ByteBuffer.allocate(32).order(ByteOrder.BIG_ENDIAN);

      int value = 0;
      for (int i = attributes.nextSetBit(0); i >= 0; i = attributes.nextSetBit(i + 1)) {
        value |= (1 << i);
      }
      buffer.putInt(value);

      return buffer.array();
    }
  }
}
