package com.duyvu.database.tree;

import com.duyvu.database.schema.Type;
import com.duyvu.database.schema.TypeLengthValue;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import static com.duyvu.database.utils.Constants.META_DATA_LENGTH;

@Getter
public class InternalNode extends Node {
  private final List<Key> keys;
  private final List<Long> childrenIds;

  public InternalNode(long pageId, List<Key> keys, List<Long> childrenIds) {
    super(pageId);
    this.keys = keys;
    this.childrenIds = childrenIds;
  }

  @Override
  public Type getType() {
    return Type.INTERNAL_NODE;
  }

  @Override
  public byte[] getValue() {
    ByteBuffer buffer =
        ByteBuffer.allocate(
                (Long.BYTES + META_DATA_LENGTH) // page id
                    + (Long.BYTES + META_DATA_LENGTH) * childrenIds.size() // children ids
                    + keys.stream()
                        .map(TypeLengthValue::getLength)
                        .mapToInt(e -> e + META_DATA_LENGTH)
                        .sum()) // keys
            .order(ByteOrder.BIG_ENDIAN);

    buffer.put(Type.LONG.getCode());
    buffer.putInt(Long.BYTES);
    buffer.putLong(pageId);

    for (long id : childrenIds) {
      buffer.put(Type.LONG.getCode());
      buffer.putInt(Long.BYTES);
      buffer.putLong(id);
    }

    for (Key key : keys) {
      buffer.put(key.getType().getCode());
      buffer.putInt(key.getLength());
      buffer.put(key.val());
    }

    return buffer.array();
  }
}
