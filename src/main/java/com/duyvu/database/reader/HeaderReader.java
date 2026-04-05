package com.duyvu.database.reader;

import com.duyvu.database.schema.ColumnDefinition;
import com.duyvu.database.schema.Header;
import com.duyvu.database.schema.Type;
import lombok.SneakyThrows;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static com.duyvu.database.utils.Constants.META_DATA_LENGTH;

public class HeaderReader implements Reader<RandomAccessFile, Header> {

  @Override
  @SneakyThrows
  public Header read(RandomAccessFile raf) {
    raf.seek(0);
    byte[] headerMetaData = new byte[META_DATA_LENGTH];
    raf.read(headerMetaData);
    ByteBuffer headerMetaDataBuffer = ByteBuffer.wrap(headerMetaData).order(ByteOrder.BIG_ENDIAN);

    Type type = Type.fromCode(headerMetaDataBuffer.get());
    if (type != Type.HEADER) {
      throw new IllegalArgumentException("Invalid type");
    }
    // Skip length
    int length = headerMetaDataBuffer.getInt();
    byte[] headerValue = new byte[length];
    raf.read(headerValue);
    ByteBuffer headerBuffer = ByteBuffer.wrap(headerValue);

    List<ColumnDefinition> columnDefinitions = new ArrayList<>();
    ColumnDefinitionReader converter = new ColumnDefinitionReader();
    while (headerBuffer.hasRemaining()) {
      Type columnType = Type.fromCode(headerBuffer.get());
      if (columnType != Type.COLUMN_DEFINITION) {
        throw new IllegalArgumentException("Invalid column definition type");
      }
      int columnLength = headerBuffer.getInt();
      byte[] columnValue = new byte[columnLength];
      headerBuffer.get(columnValue);

      columnDefinitions.add(converter.read(columnValue));
    }

    return new Header(columnDefinitions);
  }

  static class ColumnDefinitionReader implements Reader<byte[], ColumnDefinition> {
    @Override
    public ColumnDefinition read(byte[] data) {
      ByteBuffer buffer = ByteBuffer.wrap(data);

      Type nameType = Type.fromCode(buffer.get());
      if (nameType != Type.STRING) {
        throw new IllegalArgumentException("Invalid type");
      }
      int nameLength = buffer.getInt();
      byte[] nameValue = new byte[nameLength];
      buffer.get(nameValue);
      ColumnDefinition.ColumnName columnName =
          new ColumnDefinition.ColumnName(new String(nameValue));

      Type typeType = Type.fromCode(buffer.get());
      if (typeType != Type.BYTE) {
        throw new IllegalArgumentException("Invalid type");
      }
      int typeLength = buffer.getInt();
      byte[] typeValue = new byte[typeLength];
      buffer.get(typeValue);
      ColumnDefinition.ColumnType columnType =
          new ColumnDefinition.ColumnType(Type.fromCode(typeValue[0]));

      Type attributeType = Type.fromCode(buffer.get());
      if (attributeType != Type.INT) {
        throw new IllegalArgumentException("Invalid type");
      }
      // Skip length as fixed length
      buffer.getInt();
      int attribute = buffer.getInt();
      ColumnDefinition.ColumnAttribute columnAttribute =
          new ColumnDefinition.ColumnAttribute(attribute);

      return new ColumnDefinition(columnName, columnType, columnAttribute);
    }
  }
}
