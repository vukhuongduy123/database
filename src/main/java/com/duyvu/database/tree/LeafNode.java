package com.duyvu.database.tree;

import com.duyvu.database.schema.Type;
import com.duyvu.database.schema.TypeLengthValue;
import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.util.List;

import static com.duyvu.database.utils.Constants.META_DATA_LENGTH;

@Getter
@Setter
public class LeafNode extends Node implements TypeLengthValue {
  private List<KeyValue> keyValues;
  private long previousNodeId;
  private long nextNodeId;

  public LeafNode(long pageId, long previousNodeId, long nextNodeId, List<KeyValue> keyValues) {
    super(pageId);
    this.previousNodeId = previousNodeId;
    this.nextNodeId = nextNodeId;
    this.keyValues = keyValues;
  }

  @Override
  public Type getType() {
    return Type.LEAF_NODE;
  }

  @Override
  public byte[] getValue() {
    ByteBuffer buffer = ByteBuffer.allocate((Long.BYTES + META_DATA_LENGTH) // page id
        + (Long.BYTES + META_DATA_LENGTH) // next page id
        + (Long.BYTES + META_DATA_LENGTH) // previous page id
        + keyValues.stream().map(TypeLengthValue::getLength).mapToInt(e -> e + META_DATA_LENGTH).sum()); // key values

    buffer.put(Type.LONG.getCode());
    buffer.putInt(Long.BYTES);
    buffer.putLong(pageId);

    buffer.put(Type.LONG.getCode());
    buffer.putInt(Long.BYTES);
    buffer.putLong(previousNodeId);

    buffer.put(Type.LONG.getCode());
    buffer.putInt(Long.BYTES);
    buffer.putLong(nextNodeId);

    for (KeyValue keyValue : keyValues) {
      buffer.put(keyValue.getType().getCode());
      buffer.putInt(keyValue.getLength());
      buffer.put(keyValue.getValue());
    }

    return buffer.array();
  }
}
