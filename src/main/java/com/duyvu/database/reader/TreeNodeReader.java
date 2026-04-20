package com.duyvu.database.reader;

import com.duyvu.database.schema.Type;
import com.duyvu.database.tree.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TreeNodeReader implements Reader<ByteBuffer, Node> {
  @Override
  public Node read(ByteBuffer data) {
    Type type = Type.fromCode(data.get());
    if (type != Type.INTERNAL_NODE && type != Type.LEAF_NODE) {
      throw new IllegalArgumentException("Invalid type");
    }

    switch (type) {
      case Type.INTERNAL_NODE -> {
        InternalNodeReader internalNodeReader = new InternalNodeReader();
        return internalNodeReader.read(data);
      }
      case Type.LEAF_NODE -> {
        LeafNodeReader leafNodeReader = new LeafNodeReader();
        return leafNodeReader.read(data);
      }
      default -> throw new IllegalArgumentException("Invalid type");
    }
  }

  public static class InternalNodeReader implements Reader<ByteBuffer, InternalNode> {
    @Override
    public InternalNode read(ByteBuffer data) {
      int size = data.getInt();
      ByteBuffer buffer = data.slice().limit(size);
      List<Long> childrenIds = new ArrayList<>();
      List<Key> keys = new ArrayList<>();

      Type type = Type.fromCode(buffer.get());
      if (type != Type.LONG) {
        throw new IllegalArgumentException("Invalid type");
      }
      // skip page id length
      buffer.getInt();
      long pageId = buffer.getLong();

      while (buffer.hasRemaining()) {
        type = Type.fromCode(buffer.get());
        if (type != Type.LONG && type != Type.KEY) {
          throw new IllegalArgumentException("Invalid type");
        }
        if (type == Type.LONG) {
          // skip length
          buffer.getInt();
          childrenIds.add(buffer.getLong());
        } else {
          int keySize = buffer.getInt();
          byte[] keyVals = new byte[keySize];
          buffer.get(keyVals);
          keys.add(new Key(ByteBuffer.wrap(keyVals)));
        }
      }

      return new InternalNode(pageId, keys, childrenIds);
    }
  }

  public static class LeafNodeReader implements Reader<ByteBuffer, LeafNode> {
    @Override
    public LeafNode read(ByteBuffer data) {
      int size = data.getInt();
      ByteBuffer buffer = data.slice().limit(size);
      List<KeyValue> keyValues = new ArrayList<>();

      Type type = Type.fromCode(buffer.get());
      if (type != Type.LONG) {
        throw new IllegalArgumentException("Invalid type");
      }
      // skip page id length
      buffer.getInt();
      long pageId = buffer.getLong();

      type = Type.fromCode(buffer.get());
      if (type != Type.LONG) {
        throw new IllegalArgumentException("Invalid type");
      }
      // skip previous page id length
      buffer.getInt();
      long previousNodeId = buffer.getLong();

      type = Type.fromCode(buffer.get());
      if (type != Type.LONG) {
        throw new IllegalArgumentException("Invalid type");
      }
      // skip next page id length
      buffer.getInt();
      long nextNodeId = buffer.getLong();
      
      while (buffer.hasRemaining()) {
        type = Type.fromCode(buffer.get());
        if (type != Type.KEY_VALUE) {
          throw new IllegalArgumentException("Invalid type");
        }
        buffer.getInt(); // skip key value length
        
        type = Type.fromCode(buffer.get());
        if (type != Type.KEY) {
          throw new IllegalArgumentException("Invalid type");
        }
        int keySize = buffer.getInt();
        byte[] keyVals = new byte[keySize];
        buffer.get(keyVals);

        type = Type.fromCode(buffer.get());
        if (type != Type.VALUE) {
          throw new IllegalArgumentException("Invalid type");
        }
        int valueSize = buffer.getInt();
        byte[] valueVals = new byte[valueSize];
        buffer.get(valueVals);

        keyValues.add(
            new KeyValue(new Key(ByteBuffer.wrap(keyVals)), new Value(ByteBuffer.wrap(valueVals))));
      }

      return new LeafNode(pageId, previousNodeId, nextNodeId, keyValues);
    }
  }
}
