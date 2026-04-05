package com.duyvu.database.reader;

import com.duyvu.database.exception.DatabaseException;
import com.duyvu.database.exception.ErrorCode;
import com.duyvu.database.schema.PageOffsetLength;
import com.duyvu.database.schema.Type;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import java.io.RandomAccessFile;

@Log4j2
public class PageOffsetLengthReader implements Reader<RandomAccessFile, PageOffsetLength> {
  @Override
  @SneakyThrows
  public PageOffsetLength read(RandomAccessFile file) {
    long offset = file.getFilePointer();
    Type type = Type.fromCode(file.readByte());
    if (type != Type.PAGE) {
      throw new DatabaseException(ErrorCode.INVALID_VALUE_TYPE, "Expected %s type, got: %s".formatted(Type.PAGE, type));
    }
    
    int length = file.readInt();
    return new PageOffsetLength(offset, length);
  }
}
