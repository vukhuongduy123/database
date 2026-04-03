package com.duyvu.database.schema;

import static com.duyvu.database.schema.Type.HEADER;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public record Header(List<ColumnDefinition> columnDefinitions) implements TypeLengthValue {

  @Override
  public Type getType() {
    return HEADER;
  }

  @Override
  public int getLength() {
    return getValue().length;
  }

  @Override
  public byte[] getValue() {
    int size = columnDefinitions.stream().mapToInt(e -> e.getLength() + META_DATA_LENGTH).sum();
    ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
    for (ColumnDefinition columnDefinition : columnDefinitions) {
      buffer.put(columnDefinition.getType().getCode());
      buffer.putInt(columnDefinition.getLength());
      buffer.put(columnDefinition.getValue());
    }
    return buffer.array();
  }
}
