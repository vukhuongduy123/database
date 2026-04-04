package com.duyvu.database.schema;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public record RecordsValue(List<RecordValue> recordValues) implements TypeLengthValue {
  @Override
  public Type getType() {
    return Type.RECORD;
  }

  @Override
  public int getLength() {
    return getValue().length;
  }

  @Override
  public byte[] getValue() {
    int size = recordValues.stream().mapToInt(e -> e.getLength() + META_DATA_LENGTH).sum();
    ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
    for (RecordValue recordValue : recordValues) {
      buffer.put(recordValue.getType().getCode());
      buffer.putInt(recordValue.getLength());
      buffer.put(recordValue.getValue());
    }

    return buffer.array();
  }
}
