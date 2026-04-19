package com.duyvu.database.tree;

import com.duyvu.database.reader.TreeNodeReader;
import com.duyvu.database.reader.TypeLengthValueReader;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import static com.duyvu.database.utils.Constants.B_TREE_NODE_SIZE;

@RequiredArgsConstructor
class Pager {
  private final RandomAccessFile raf;

  @SneakyThrows
  public boolean isEmpty() {
    return raf.length() == 0;
  }

  @SneakyThrows
  public Node readPage(long pageId) {
    raf.seek(pageId);

    byte[] nodeBytes = new byte[B_TREE_NODE_SIZE];
    raf.read(nodeBytes);

    ByteBuffer buffer = ByteBuffer.wrap(nodeBytes);
    TreeNodeReader reader = new TreeNodeReader();
    return reader.read(buffer);
  }

  @SneakyThrows
  public void writePage(long pageId, Node node) {
    raf.seek(pageId);
    TypeLengthValueReader reader = new TypeLengthValueReader();
    raf.write(reader.read(node));
  }

  @SneakyThrows
  public long nextPageId() {
    return raf.length() / B_TREE_NODE_SIZE + 1;
  }
}
