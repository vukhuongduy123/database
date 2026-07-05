package com.duyvu.database.schema;

import static com.duyvu.database.utils.Constants.META_DATA_LENGTH;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
    private int attributes;
    public static final int NO_ATTRIBUTE = 1; // 1
    public static final int PRIMARY_KEY = 1 << 1; // 2
    public static final int INDEX = 1 << 2; // 4

    public ColumnAttribute(int value) {
      attributes = value;
    }

    @Override
    public Type getType() {
      return Type.INT;
    }

    @Override
    public int getLength() {
      return getValue().length;
    }

    public boolean isIndex() {
      return (attributes & INDEX) != 0 || (attributes & PRIMARY_KEY) != 0;
    }

    @Override
    public byte[] getValue() {
      ByteBuffer buffer = ByteBuffer.allocate(32).order(ByteOrder.BIG_ENDIAN);
      buffer.putInt(attributes);

      return buffer.array();
    }
  }
}
