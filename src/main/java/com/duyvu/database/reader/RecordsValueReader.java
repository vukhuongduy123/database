package com.duyvu.database.reader;

import com.duyvu.database.exception.DatabaseException;
import com.duyvu.database.exception.ErrorCode;
import com.duyvu.database.schema.RecordValue;
import com.duyvu.database.schema.RecordsValue;
import com.duyvu.database.schema.Type;
import lombok.SneakyThrows;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RecordsValueReader implements Reader<ByteBuffer, RecordsValue> {
  private final long baseOffset;

  public RecordsValueReader(long baseOffset) {
    this.baseOffset = baseOffset;
  }

  @Override
  @SneakyThrows
  public RecordsValue read(ByteBuffer buffer) {
    long recordOffset = baseOffset + buffer.position();
    Type type = Type.fromCode(buffer.get());
    if (type != Type.RECORD && type != Type.DELETED_RECORD) {
      throw new DatabaseException(ErrorCode.INVALID_VALUE_TYPE);
    }
    // get length
    int recordSize = buffer.getInt();
    
    List<RecordValue> recordValues = new ArrayList<>();
    ByteBuffer recordBuffer = buffer.slice().limit(recordSize);
    while (recordBuffer.hasRemaining()) {
      Type recordType = Type.fromCode(recordBuffer.get());
      int recordLength = recordBuffer.getInt();
      Object value;
      switch (recordType) {
        case STRING:
          byte[] bytes = new byte[recordLength];
          recordBuffer.get(bytes);
          value = new String(bytes);
          break;
        case INT:
          value = recordBuffer.getInt();
          break;
        case LONG:
          value = recordBuffer.getLong();
          break;
        case DOUBLE:
          value = recordBuffer.getDouble();
          break;
        case FLOAT:
          value = recordBuffer.getFloat();
          break;
        default:
          throw new IllegalArgumentException("Unsupported type: " + type);
      }
      recordValues.add(new RecordValue(value));
    }
    buffer.position(buffer.position() + recordSize);
    return new RecordsValue(type, recordValues, recordOffset);
  }
}
