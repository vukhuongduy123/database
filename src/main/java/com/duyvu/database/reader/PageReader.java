package com.duyvu.database.reader;

import com.duyvu.database.exception.DatabaseException;
import com.duyvu.database.exception.ErrorCode;
import com.duyvu.database.schema.Page;
import com.duyvu.database.schema.RecordsValue;
import com.duyvu.database.schema.Type;
import lombok.SneakyThrows;

import java.io.RandomAccessFile;

import static com.duyvu.database.utils.Constants.META_DATA_LENGTH;

public class PageReader implements Reader<RandomAccessFile, Page> {

  @Override
  @SneakyThrows
  public Page read(RandomAccessFile raf) {
    long pageOffset = raf.getFilePointer();
    Type type = Type.fromCode(raf.readByte());
    if (type != Type.PAGE) {
      throw new DatabaseException(
          ErrorCode.INVALID_VALUE_TYPE, "Expected %s type, got: %s".formatted(Type.PAGE, type));
    }

    int length = raf.readInt();
    byte[] pageBytes = new byte[length];
    raf.read(pageBytes);
    RecordsValueReader reader = new RecordsValueReader();
    RecordsValue recordsValue = reader.read(raf);
    raf.skipBytes(length - recordsValue.getLength() - META_DATA_LENGTH);

    return new Page(recordsValue, pageOffset);
  }
}
