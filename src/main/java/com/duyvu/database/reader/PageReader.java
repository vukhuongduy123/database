package com.duyvu.database.reader;

import com.duyvu.database.exception.DatabaseException;
import com.duyvu.database.exception.ErrorCode;
import com.duyvu.database.schema.Page;
import com.duyvu.database.schema.RecordsValue;
import com.duyvu.database.schema.Type;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.duyvu.database.utils.Constants.PAGE_SIZE;

@Log4j2
public class PageReader implements Reader<RandomAccessFile, Page> {

  @Override
  @SneakyThrows
  public Page read(RandomAccessFile raf) {
    long pageOffset = raf.getFilePointer();
    byte[] pageBytes = new byte[PAGE_SIZE];
    raf.read(pageBytes);
    ByteBuffer buffer = ByteBuffer.wrap(pageBytes);
    
    Type type = Type.fromCode(buffer.get());
    if (type != Type.PAGE) {
      throw new DatabaseException(
          ErrorCode.INVALID_VALUE_TYPE, "Expected %s type, got: %s".formatted(Type.PAGE, type));
    }

    int length = buffer.getInt();
    ByteBuffer recordBuffer = buffer.slice().limit(length);
    
    long baseOffset = pageOffset + buffer.position();
    RecordsValueReader reader = new RecordsValueReader(baseOffset);
    
    List<RecordsValue> recordsValues = new ArrayList<>();
    while (recordBuffer.hasRemaining()) {
      try {
        RecordsValue recordsValue = reader.read(recordBuffer);
        recordsValues.add(recordsValue);
      } catch (DatabaseException e) {
        if (e.getErrorCode() != ErrorCode.INVALID_VALUE_TYPE) {
          log.error("Error reading records value", e);
          throw e;
        }
      }
    }

    return new Page(recordsValues, pageOffset);
  }
}
