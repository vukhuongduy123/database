package com.duyvu.database.schema;

import static com.duyvu.database.schema.Type.HEADER;
import static com.duyvu.database.utils.Constants.META_DATA_LENGTH;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;

@Getter
public class Header implements TypeLengthValue {
  private final List<ColumnDefinition> columnDefinitions;
  private final Map<String, ColumnDefinition> columnDefinitionMap;

  public Header(List<ColumnDefinition> columnDefinitions) {
    this.columnDefinitions = columnDefinitions;
    columnDefinitionMap =
        columnDefinitions.stream()
            .collect(Collectors.toMap(cd -> cd.columnName().getName(), cd -> cd));
  }

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
