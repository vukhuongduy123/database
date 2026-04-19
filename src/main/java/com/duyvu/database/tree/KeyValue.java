package com.duyvu.database.tree;

import com.duyvu.database.schema.Type;
import com.duyvu.database.schema.TypeLengthValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.duyvu.database.utils.Constants.META_DATA_LENGTH;

public record KeyValue(Key key, Value value) implements TypeLengthValue {
  @Override
  public Type getType() {
    return Type.KEY_VALUE;
  }

  @Override
  public byte[] getValue() {
    // key length + value length + meta data for key and value
    ByteBuffer buffer = ByteBuffer.allocate(key.getLength() + value.getLength() + META_DATA_LENGTH * 2).order(ByteOrder.BIG_ENDIAN);

    buffer.put(key.getType().getCode());
    buffer.putInt(key.getLength());
    buffer.put(key.getValue());

    buffer.put(value.getType().getCode());
    buffer.putInt(value.getLength());
    buffer.put(value.getValue());
    return buffer.array();
  }
}
