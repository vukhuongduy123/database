package com.duyvu.database.schema;

import lombok.Data;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.duyvu.database.utils.Constants.META_DATA_LENGTH;
import static com.duyvu.database.utils.Constants.PAGE_SIZE;

@Data
public class Page implements TypeLengthValue {
  private long offset;
  private RecordsValue recordValues;

  public Page(RecordsValue recordValues, long offset) {
    this.recordValues = recordValues;
    this.offset = offset;
  }

  @Override
  public Type getType() {
    return Type.PAGE;
  }

  @Override
  public byte[] getValue() {
    ByteBuffer buffer =
        ByteBuffer.allocate(recordValues.getLength() + META_DATA_LENGTH)
            .order(ByteOrder.BIG_ENDIAN);
    buffer.put(recordValues.getType().getCode());
    buffer.putInt(recordValues.getLength());
    buffer.put(recordValues.getValue());

    return buffer.array();
  }

  public int getUnusedSpace() {
    return PAGE_SIZE - getValue().length;
  }
}
