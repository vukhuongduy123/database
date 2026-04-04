package com.duyvu.database.reader;

import com.duyvu.database.schema.RecordValue;
import com.duyvu.database.schema.RecordsValue;
import com.duyvu.database.schema.Type;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;

public class RecordsValueReader implements Reader<RandomAccessFile, RecordsValue> {
  @Override
  @SneakyThrows
  public RecordsValue read(RandomAccessFile raf) {
    long recordOffset = raf.getFilePointer();
    Type type = Type.fromCode(raf.readByte());
    if (type != Type.RECORD && type != Type.DELETED_RECORD) {
      throw new IllegalArgumentException("Invalid type");
    }
    int length = raf.readInt();
    if (type == Type.DELETED_RECORD) {
      raf.skipBytes(length);
      return new RecordsValue(type, List.of(), recordOffset);
    }

    byte[] recordBytes = new byte[length];
    int readBytes = raf.read(recordBytes);
    if (readBytes < 0) {
      throw new IllegalStateException("Invalid record length");
    }

    ByteBuffer buffer = ByteBuffer.wrap(recordBytes);
    List<RecordValue> recordValues = new ArrayList<>();
    while (buffer.hasRemaining()) {
      Type recordType = Type.fromCode(buffer.get());
      int recordLength = buffer.getInt();
      Object value;
      switch (recordType) {
        case STRING:
          byte[] bytes = new byte[recordLength];
          buffer.get(bytes);
          value = new String(bytes);
          break;
        case INT:
          value = buffer.getInt();
          break;
        case LONG:
          value = buffer.getLong();
          break;
        case DOUBLE:
          value = buffer.getDouble();
          break;
        case FLOAT:
          value = buffer.getFloat();
          break;
        default:
          throw new IllegalArgumentException("Unsupported type: " + type);
      }
      recordValues.add(new RecordValue(value));
    }
    return new RecordsValue(type, recordValues, recordOffset);
  }
}
