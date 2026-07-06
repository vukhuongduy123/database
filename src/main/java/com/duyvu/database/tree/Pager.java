package com.duyvu.database.tree;

import static com.duyvu.database.utils.Constants.B_TREE_NODE_SIZE;

import com.duyvu.database.reader.TreeNodeReader;
import com.duyvu.database.reader.TypeLengthValueReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

@Log4j2
class Pager {
  private final FileChannel fileChannel;
  private final ByteBuffer pageBuffer = ByteBuffer.allocateDirect(B_TREE_NODE_SIZE).order(ByteOrder.BIG_ENDIAN);

  private static final byte[] ZERO_PAD = new byte[B_TREE_NODE_SIZE];

  Pager(RandomAccessFile raf) {
    this.fileChannel = raf.getChannel();
  }

  @SneakyThrows
  public boolean isEmpty() {
    return fileChannel.size() == 0;
  }

  @SneakyThrows
  public Node readPage(long pageId) {
    fileChannel.position(pageId);

    pageBuffer.clear();
    readFully(pageBuffer, pageId);
    pageBuffer.flip();

    TreeNodeReader reader = new TreeNodeReader();
    return reader.read(pageBuffer);
  }

  @SneakyThrows
  public void writePage(long pageId, Node node) {
    if (node.getLength() > B_TREE_NODE_SIZE) {
      throw new IllegalArgumentException("Node size is too large");
    }

    pageBuffer.clear();

    TypeLengthValueReader reader = new TypeLengthValueReader();
    reader.readInto(node, pageBuffer);
    pageBuffer.put(ZERO_PAD, 0, B_TREE_NODE_SIZE - pageBuffer.position());
    writeFully(pageBuffer, pageId);
    pageBuffer.flip();
  }

  @SneakyThrows
  public long nextPageId() {
    return fileChannel.size();
  }

  @SneakyThrows
  private void readFully(ByteBuffer buffer, long position) {
    long currentPosition = position;
    while (buffer.hasRemaining()) {
      int n = fileChannel.read(buffer, currentPosition);
      if (n < 0) {
        return;
      }
      currentPosition += n;
    }
  }

  @SneakyThrows
  private void writeFully(ByteBuffer buffer, long position) {
    long currentPosition = position;
    while (buffer.hasRemaining()) {
      int n = fileChannel.write(buffer, currentPosition);
      if (n < 0) {
        log.warn("Failed to write to file channel at position {}", currentPosition);
        return;
      }
      currentPosition += n;
    }
  }
}
