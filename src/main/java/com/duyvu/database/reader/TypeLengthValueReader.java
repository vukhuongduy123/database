package com.duyvu.database.reader;

import static com.duyvu.database.utils.Constants.META_DATA_LENGTH;

import com.duyvu.database.schema.TypeLengthValue;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TypeLengthValueReader implements Reader<TypeLengthValue, byte[]> {
  @Override
  public byte[] read(TypeLengthValue tlv) {
    ByteBuffer buffer =
        ByteBuffer.allocate(META_DATA_LENGTH + tlv.getLength()).order(ByteOrder.BIG_ENDIAN);

    buffer.put(tlv.getType().getCode());
    buffer.putInt(tlv.getLength());
    buffer.put(tlv.getValue());

    return buffer.array();
  }
}
