package com.duyvu.database.schema;

import lombok.Data;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import static com.duyvu.database.utils.Constants.META_DATA_LENGTH;
import static com.duyvu.database.utils.Constants.PAGE_SIZE;

@Data
public class Page implements TypeLengthValue {
  private long offset;
  private List<RecordsValue> recordsValues;

  public Page(List<RecordsValue> recordsValues, long offset) {
    this.recordsValues = recordsValues;
    this.offset = offset;
  }

  @Override
  public Type getType() {
    return Type.PAGE;
  }

  @Override
  public byte[] getValue() {
    int size = recordsValues.stream().mapToInt(e -> e.getLength() + META_DATA_LENGTH).sum();
    ByteBuffer buffer =
        ByteBuffer.allocate(size + META_DATA_LENGTH)
            .order(ByteOrder.BIG_ENDIAN);
    for (RecordsValue recordValues : recordsValues) {
      buffer.put(recordValues.getType().getCode());
      buffer.putInt(recordValues.getLength());
      buffer.put(recordValues.getValue());
    }
    
    return buffer.array();
  }

  public int getUnusedSpace() {
    return PAGE_SIZE - getValue().length;
  }
}
