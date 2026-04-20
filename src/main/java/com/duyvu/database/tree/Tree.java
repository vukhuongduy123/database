package com.duyvu.database.tree;

import com.duyvu.database.schema.Type;
import com.duyvu.database.utils.FileHandler;
import com.duyvu.database.utils.PathUtils;
import com.duyvu.database.utils.SearchUtils;
import lombok.extern.log4j.Log4j2;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static com.duyvu.database.utils.Constants.B_TREE_ROOT_NODE_ID;
import static com.duyvu.database.utils.Constants.B_TREE_UNKNOWN_NODE_ID;

@Log4j2
public final class Tree {
  private final Pager pager;
  static final int ORDER = 100;
  static final int MAX_KEYS = ORDER - 1;
  static final int MIN_KEYS = (int) (Math.ceil(ORDER / 2.0) - 1);
  static final int MAX_KEY_SIZE = 100;
  static final int MAX_VALUE_SIZE = 1000;

  private record NodePath(InternalNode node, int childIndex) {}

  public Tree(Path path) {
    try {
      PathUtils.createFileIfNotExists(path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    FileHandler.getInstance().addFileHandler(path);
    pager = new Pager(FileHandler.getInstance().getFileHandler(path));
  }

  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  private boolean isFull(Node node) {
    switch (node.getType()) {
      case INTERNAL_NODE -> {
        return ((InternalNode) node).getKeys().size() > MAX_KEYS;
      }
      case LEAF_NODE -> {
        return ((LeafNode) node).getKeyValues().size() > MAX_KEYS;
      }
      default -> throw new IllegalArgumentException("Unsupported node type: " + node.getType());
    }
  }

  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  private boolean isUnderflow(Node node) {
    switch (node.getType()) {
      case INTERNAL_NODE -> {
        return ((InternalNode) node).getKeys().size() <= MIN_KEYS;
      }
      case LEAF_NODE -> {
        return ((LeafNode) node).getKeyValues().size() <= MIN_KEYS;
      }
      default -> throw new IllegalArgumentException("Unsupported node type: " + node.getType());
    }
  }

  public void delete(Key key) {
    if (pager.isEmpty()) {
      return;
    }

    Node node = pager.readPage(B_TREE_ROOT_NODE_ID);

    Deque<NodePath> paths = new ArrayDeque<>();

    while (node.getType() == Type.INTERNAL_NODE) {
      InternalNode internalNode = (InternalNode) node;
      SearchResult searchResult = SearchUtils.search(internalNode.getKeys(), key);
      int idx = searchResult.found() ? searchResult.index() + 1 : searchResult.index();
      paths.push(new NodePath(internalNode, idx));

      node = pager.readPage(internalNode.getChildrenIds().get(idx));
    }

    LeafNode leaf = (LeafNode) node;

    // search inside leaf
    List<KeyValue> keyValues = leaf.getKeyValues();
    SearchResult searchResult =
        SearchUtils.search(keyValues.stream().map(KeyValue::key).toList(), key);
    if (!searchResult.found()) {
      return;
    }

    keyValues.remove(searchResult.index());
    if (!isUnderflow(leaf)) {
      pager.writePage(leaf.getPageId(), leaf);
      return;
    }

    if (leaf.getPreviousNodeId() != B_TREE_UNKNOWN_NODE_ID) {
      LeafNode leftNode = (LeafNode) pager.readPage(leaf.getPreviousNodeId());
      if (canBorrowFromSibling(leftNode)) {
        NodePath nodePath = paths.pop();
        InternalNode parent = nodePath.node;
        int leafIndex = nodePath.childIndex;
        borrowFromLeftSibling(parent, leafIndex, leaf, leftNode);
        return;
      }
    }

    if (leaf.getNextNodeId() != B_TREE_UNKNOWN_NODE_ID) {
      LeafNode rightNode = (LeafNode) pager.readPage(leaf.getNextNodeId());
      if (canBorrowFromSibling(rightNode)) {
        NodePath nodePath = paths.pop();
        InternalNode parent = nodePath.node;
        int leafIndex = nodePath.childIndex;
        borrowFromRightSibling(parent, leafIndex, leaf, rightNode);
        return;
      }
    }

    NodePath nodePath = paths.pop();
    InternalNode parent = nodePath.node;
    int leafIndex = nodePath.childIndex;

    // Prefer merge with left
    if (leafIndex > 0) {
      LeafNode leftNode = (LeafNode) pager.readPage(leaf.getPreviousNodeId());
      mergeWithLeftSibling(parent, leafIndex, leaf, leftNode, paths);
    } else {
      LeafNode rightNode = (LeafNode) pager.readPage(leaf.getNextNodeId());
      mergeWithRightSibling(parent, leafIndex, leaf, rightNode, paths);
    }
  }

  private void mergeWithLeftSibling(
      InternalNode parent, int leafIndex, LeafNode leaf, LeafNode left, Deque<NodePath> paths) {
    // 1. Move all keys from leaf → left
    left.getKeyValues().addAll(leaf.getKeyValues());

    // 2. Fix linked list
    left.setNextNodeId(leaf.getNextNodeId());
    if (leaf.getNextNodeId() != B_TREE_UNKNOWN_NODE_ID) {
      LeafNode next = (LeafNode) pager.readPage(leaf.getNextNodeId());
      next.setPreviousNodeId(left.getPageId());
      pager.writePage(next.getPageId(), next);
    }

    // 3. Remove the child pointer from its parent
    parent.getChildrenIds().remove(leafIndex);

    // 4. Remove separator key
    parent.getKeys().remove(leafIndex - 1);

    // 5. Persist
    pager.writePage(left.getPageId(), left);
    pager.writePage(parent.getPageId(), parent);

    // 6. Handle parent underflow
    handleInternalUnderflow(parent, paths);
  }

  private void mergeWithRightSibling(
      InternalNode parent, int leafIndex, LeafNode leaf, LeafNode right, Deque<NodePath> paths) {
    // 1. Move all keys from right → leaf
    leaf.getKeyValues().addAll(right.getKeyValues());

    // 2. Fix linked list
    leaf.setNextNodeId(right.getNextNodeId());
    if (right.getNextNodeId() != B_TREE_UNKNOWN_NODE_ID) {
      LeafNode next = (LeafNode) pager.readPage(right.getNextNodeId());
      next.setPreviousNodeId(leaf.getPageId());
      pager.writePage(next.getPageId(), next);
    }

    // 3. Remove right child from parent
    parent.getChildrenIds().remove(leafIndex + 1);

    // 4. Remove separator key
    parent.getKeys().remove(leafIndex);

    // 5. Persist
    pager.writePage(leaf.getPageId(), leaf);
    pager.writePage(parent.getPageId(), parent);

    // 6. Handle parent underflow
    handleInternalUnderflow(parent, paths);
  }

  private void handleInternalUnderflow(InternalNode node, Deque<NodePath> paths) {
    InternalNode cur = node;
    while (true) {
      // Root special case
      if (cur.getPageId() == B_TREE_ROOT_NODE_ID) {
        if (cur.getKeys().isEmpty()) {
          long newRootId = cur.getChildrenIds().getFirst();
          Node newRoot = pager.readPage(newRootId);
          newRoot.setPageId(B_TREE_ROOT_NODE_ID);
          pager.writePage(B_TREE_ROOT_NODE_ID, newRoot);
        }
        return;
      }

      if (!isUnderflow(cur)) {
        pager.writePage(cur.getPageId(), cur);
        return;
      }

      NodePath path = paths.pop();
      InternalNode parent = path.node;
      int index = path.childIndex;

      // --- Try borrow from LEFT ---
      if (index > 0) {
        InternalNode left = (InternalNode) pager.readPage(parent.getChildrenIds().get(index - 1));

        if (canBorrowFromSibling(left)) {
          borrowFromLeftInternal(parent, index, cur, left);
          return;
        }
      }

      // --- Try borrow from RIGHT ---
      if (index < parent.getChildrenIds().size() - 1) {
        InternalNode right = (InternalNode) pager.readPage(parent.getChildrenIds().get(index + 1));

        if (canBorrowFromSibling(right)) {
          borrowFromRightInternal(parent, index, cur, right);
          return;
        }
      }

      // --- Merge ---
      if (index > 0) {
        InternalNode left = (InternalNode) pager.readPage(parent.getChildrenIds().get(index - 1));
        mergeWithLeftInternal(parent, index, cur, left);
        cur = parent; // propagate upward
      } else {
        InternalNode right = (InternalNode) pager.readPage(parent.getChildrenIds().get(index + 1));
        mergeWithRightInternal(parent, index, cur, right);
        cur = parent; // propagate upward
      }
    }
  }

  private void borrowFromLeftInternal(
      InternalNode parent, int index, InternalNode node, InternalNode left) {
    // bring separator down
    Key separator = parent.getKeys().get(index - 1);

    // move from left → parent
    Key borrowedKey = left.getKeys().removeLast();
    long borrowedChild = left.getChildrenIds().removeLast();

    // update parent
    parent.getKeys().set(index - 1, borrowedKey);

    // insert into current node
    node.getKeys().addFirst(separator);
    node.getChildrenIds().addFirst(borrowedChild);

    pager.writePage(left.getPageId(), left);
    pager.writePage(node.getPageId(), node);
    pager.writePage(parent.getPageId(), parent);
  }

  private void borrowFromRightInternal(
      InternalNode parent, int index, InternalNode node, InternalNode right) {
    Key separator = parent.getKeys().get(index);

    Key borrowedKey = right.getKeys().removeFirst();
    long borrowedChild = right.getChildrenIds().removeFirst();

    parent.getKeys().set(index, borrowedKey);

    node.getKeys().add(separator);
    node.getChildrenIds().add(borrowedChild);

    pager.writePage(right.getPageId(), right);
    pager.writePage(node.getPageId(), node);
    pager.writePage(parent.getPageId(), parent);
  }

  private void mergeWithLeftInternal(
      InternalNode parent, int index, InternalNode node, InternalNode left) {
    // pull separator down
    Key separator = parent.getKeys().get(index - 1);

    left.getKeys().add(separator);
    left.getKeys().addAll(node.getKeys());
    left.getChildrenIds().addAll(node.getChildrenIds());

    // remove node from parent
    parent.getKeys().remove(index - 1);
    parent.getChildrenIds().remove(index);

    pager.writePage(left.getPageId(), left);
    pager.writePage(parent.getPageId(), parent);
  }

  private void mergeWithRightInternal(
      InternalNode parent, int index, InternalNode node, InternalNode right) {
    Key separator = parent.getKeys().get(index);

    node.getKeys().add(separator);
    node.getKeys().addAll(right.getKeys());
    node.getChildrenIds().addAll(right.getChildrenIds());

    parent.getKeys().remove(index);
    parent.getChildrenIds().remove(index + 1);

    pager.writePage(node.getPageId(), node);
    pager.writePage(parent.getPageId(), parent);
  }

  private boolean canBorrowFromSibling(Node node) {
    switch (node.getType()) {
      case INTERNAL_NODE -> {
        return ((InternalNode) node).getKeys().size() > MIN_KEYS;
      }
      case LEAF_NODE -> {
        return ((LeafNode) node).getKeyValues().size() > MIN_KEYS;
      }
      default -> throw new IllegalArgumentException("Unsupported node type: " + node.getType());
    }
  }

  private void borrowFromLeftSibling(
      InternalNode parent, int leafIndex, LeafNode leaf, LeafNode sibling) {
    KeyValue borrowed = sibling.getKeyValues().removeLast();
    leaf.getKeyValues().addFirst(borrowed);

    if (leafIndex > 0) {
      parent.getKeys().set(leafIndex - 1, leaf.getKeyValues().getFirst().key());
    }

    pager.writePage(leaf.getPageId(), leaf);
    pager.writePage(parent.getPageId(), parent);
    pager.writePage(sibling.getPageId(), sibling);
  }

  private void borrowFromRightSibling(
      InternalNode parent, int leafIndex, LeafNode leaf, LeafNode sibling) {
    KeyValue borrowed = sibling.getKeyValues().removeFirst();
    leaf.getKeyValues().addLast(borrowed);

    if (leafIndex < parent.getKeys().size()) {
      parent.getKeys().set(leafIndex, sibling.getKeyValues().getFirst().key());
    }

    pager.writePage(leaf.getPageId(), leaf);
    pager.writePage(parent.getPageId(), parent);
    pager.writePage(sibling.getPageId(), sibling);
  }

  public void insert(Key key, Value value) {
    if (key.getLength() > MAX_KEY_SIZE || value.getLength() > MAX_VALUE_SIZE) {
      throw new IllegalArgumentException(
          "Key or value is too long: " + key.getLength() + "," + value.getLength());
    }

    if (pager.isEmpty()) {
      LeafNode root =
          new LeafNode(
              B_TREE_ROOT_NODE_ID,
              B_TREE_UNKNOWN_NODE_ID,
              B_TREE_UNKNOWN_NODE_ID,
              List.of(new KeyValue(key, value)));
      pager.writePage(B_TREE_ROOT_NODE_ID, root);
      return;
    }

    Node node = pager.readPage(B_TREE_ROOT_NODE_ID);
    Deque<InternalNode> paths = new ArrayDeque<>();
    while (node.getType() == Type.INTERNAL_NODE) {
      InternalNode internalNode = (InternalNode) node;
      paths.push(internalNode);
      SearchResult searchResult = SearchUtils.search(internalNode.getKeys(), key);
      int idx = searchResult.found() ? searchResult.index() + 1 : searchResult.index();

      node = pager.readPage(internalNode.getChildrenIds().get(idx));
    }

    LeafNode leaf = (LeafNode) node;

    // search inside leaf
    List<KeyValue> keyValues = leaf.getKeyValues();
    SearchResult searchResult =
        SearchUtils.search(keyValues.stream().map(KeyValue::key).toList(), key);

    if (searchResult.found()) {
      log.warn("Duplicate key: {}", key.getValue());
      return; // no duplicates
    }

    keyValues.add(searchResult.index(), new KeyValue(key, value));

    // if not full, done
    if (!isFull(leaf)) {
      pager.writePage(leaf.getPageId(), leaf);
      return;
    }

    // split leaf
    SplitResult split = splitLeafAndWrite(leaf);

    Key promoteKey = split.promotedKey();
    long rightPageId = split.newRight().getPageId();

    // propagate up
    while (!paths.isEmpty()) {
      InternalNode parent = paths.pop();

      insertIntoInternal(parent, promoteKey, rightPageId);

      if (!isFull(parent)) {
        pager.writePage(parent.getPageId(), parent);
        return;
      }

      SplitResult internalSplit = splitInternalAndWrite(parent);

      promoteKey = internalSplit.promotedKey();
      rightPageId = internalSplit.newRight().getPageId();
    }

    Node oldRoot = pager.readPage(B_TREE_ROOT_NODE_ID);
    // move the old root to the new page
    long leftPageId = pager.nextPageId();
    oldRoot.setPageId(leftPageId);
    pager.writePage(leftPageId, oldRoot);

    // root split
    InternalNode newRoot =
        new InternalNode(
            B_TREE_ROOT_NODE_ID,
            new ArrayList<>(List.of(promoteKey)),
            new ArrayList<>(List.of(leftPageId, rightPageId)));

    pager.writePage(B_TREE_ROOT_NODE_ID, newRoot);
  }

  private void insertIntoInternal(InternalNode parent, Key promoteKey, long rightPageId) {
    SearchResult searchResult = SearchUtils.search(parent.getKeys(), promoteKey);
    if (searchResult.found()) {
      // should NOT happen if no duplicates
      log.warn("Duplicate internal key: {}", promoteKey.getValue());
    }
    int idx = searchResult.index();
    parent.getKeys().add(idx, promoteKey);
    parent.getChildrenIds().add(idx + 1, rightPageId);
  }

  private SplitResult splitLeafAndWrite(LeafNode leaf) {
    List<KeyValue> keyValues = leaf.getKeyValues();
    int size = keyValues.size();
    int mid = keyValues.size() / 2;
    if (mid == 0 || mid == size) {
      throw new IllegalStateException("Invalid split");
    }

    List<KeyValue> halfRightKeyValues = new ArrayList<>(keyValues.subList(mid, size));
    long oldNextId = leaf.getNextNodeId();
    LeafNode newRight =
        new LeafNode(pager.nextPageId(), leaf.getPageId(), oldNextId, halfRightKeyValues);

    if (oldNextId != B_TREE_UNKNOWN_NODE_ID) {
      LeafNode oldNext = (LeafNode) pager.readPage(oldNextId);
      oldNext.setPreviousNodeId(newRight.getPageId());
      pager.writePage(oldNext.getPageId(), oldNext);
    }

    leaf.setNextNodeId(newRight.getPageId());
    leaf.setKeyValues(new ArrayList<>(keyValues.subList(0, mid)));

    pager.writePage(leaf.getPageId(), leaf);
    pager.writePage(newRight.getPageId(), newRight);

    return new SplitResult(halfRightKeyValues.getFirst().key(), newRight);
  }

  private SplitResult splitInternalAndWrite(InternalNode internalNode) {
    List<Key> keys = internalNode.getKeys();
    List<Long> children = internalNode.getChildrenIds();

    int size = keys.size();
    int mid = size / 2;

    Key promoteKey = keys.get(mid);

    // --- capture right side ---
    List<Key> rightKeys = new ArrayList<>(keys.subList(mid + 1, size));
    List<Long> rightChildren = new ArrayList<>(children.subList(mid + 1, children.size()));

    // --- shrink left (in-place) ---
    keys.subList(mid, size).clear(); // remove mid and right
    children.subList(mid + 1, children.size()).clear();

    // --- create right node ---
    long rightPageId = pager.nextPageId();
    InternalNode rightNode = new InternalNode(rightPageId, rightKeys, rightChildren);

    // --- persist both ---
    pager.writePage(internalNode.getPageId(), internalNode);
    pager.writePage(rightPageId, rightNode);

    return new SplitResult(promoteKey, rightNode);
  }

  private LeafNode searchNode(Key key) {
    if (pager.isEmpty()) {
      return null;
    }

    Node node = pager.readPage(B_TREE_ROOT_NODE_ID);

    while (node != null && node.getType() == Type.INTERNAL_NODE) {
      InternalNode internalNode = (InternalNode) node;
      SearchResult searchResult = SearchUtils.search(internalNode.getKeys(), key);
      int idx = searchResult.found() ? searchResult.index() + 1 : searchResult.index();

      node = pager.readPage(internalNode.getChildrenIds().get(idx));
    }

    return (LeafNode) node;
  }

  public KeyValue search(Key key) {
    LeafNode leaf = searchNode(key);
    if (leaf == null) {
      return null;
    }
    SearchResult searchResult =
        SearchUtils.search(leaf.getKeyValues().stream().map(KeyValue::key).toList(), key);
    if (!searchResult.found()) {
      return null;
    }

    return leaf.getKeyValues().get(searchResult.index());
  }

  public List<KeyValue> greaterThan(Key key) {
    LeafNode leaf = searchNode(key);
    if (leaf == null) {
      return new ArrayList<>();
    }
    SearchResult searchResult =
        SearchUtils.search(leaf.getKeyValues().stream().map(KeyValue::key).toList(), key);
    int index = searchResult.found() ? searchResult.index() + 1 : searchResult.index();

    List<KeyValue> keyValues =
        new ArrayList<>(leaf.getKeyValues().subList(index, leaf.getKeyValues().size()));
    LeafNode current = leaf;
    while (current.getNextNodeId() != B_TREE_UNKNOWN_NODE_ID) {
      current = (LeafNode) pager.readPage(current.getNextNodeId());
      keyValues.addAll(current.getKeyValues());
    }

    return keyValues;
  }

  public List<KeyValue> greaterThanOrEqual(Key key) {
    LeafNode leaf = searchNode(key);
    if (leaf == null) {
      return new ArrayList<>();
    }
    SearchResult searchResult =
        SearchUtils.search(leaf.getKeyValues().stream().map(KeyValue::key).toList(), key);

    List<KeyValue> keyValues =
        new ArrayList<>(
            leaf.getKeyValues().subList(searchResult.index(), leaf.getKeyValues().size()));
    LeafNode current = leaf;
    while (current.getNextNodeId() != B_TREE_UNKNOWN_NODE_ID) {
      current = (LeafNode) pager.readPage(current.getNextNodeId());
      keyValues.addAll(current.getKeyValues());
    }

    return keyValues;
  }

  public List<KeyValue> lessThan(Key key) {
    LeafNode leaf = searchNode(key);
    if (leaf == null) {
      return new ArrayList<>();
    }

    SearchResult searchResult =
        SearchUtils.search(leaf.getKeyValues().stream().map(KeyValue::key).toList(), key);

    List<KeyValue> keyValues =
        new ArrayList<>(leaf.getKeyValues().subList(0, searchResult.index()));

    LeafNode current = leaf;
    while (current.getPreviousNodeId() != B_TREE_UNKNOWN_NODE_ID) {
      current = (LeafNode) pager.readPage(current.getPreviousNodeId());
      keyValues.addAll(0, current.getKeyValues());
    }

    return keyValues;
  }

  public List<KeyValue> lessThanOrEqual(Key key) {
    LeafNode leaf = searchNode(key);
    if (leaf == null) {
      return new ArrayList<>();
    }

    SearchResult searchResult =
        SearchUtils.search(leaf.getKeyValues().stream().map(KeyValue::key).toList(), key);

    int index = searchResult.found() ? searchResult.index() + 1 : searchResult.index();

    List<KeyValue> keyValues = new ArrayList<>(leaf.getKeyValues().subList(0, index));

    LeafNode current = leaf;
    while (current.getPreviousNodeId() != B_TREE_UNKNOWN_NODE_ID) {
      current = (LeafNode) pager.readPage(current.getPreviousNodeId());
      keyValues.addAll(current.getKeyValues());
    }

    // reverse once
    Collections.reverse(keyValues);
    return keyValues;
  }

  public void logTree() {
    StringBuilder sb = new StringBuilder();

    Node root = pager.readPage(B_TREE_ROOT_NODE_ID);
    Queue<Node> queue = new LinkedList<>();
    queue.add(root);

    while (!queue.isEmpty()) {
      int size = queue.size();

      for (int i = 0; i < size; i++) {
        Node node = queue.poll();

        assert node != null;
        if (node.getType() == Type.LEAF_NODE) {
          LeafNode leaf = (LeafNode) node;

          sb.append("[");
          for (KeyValue kv : leaf.getKeyValues()) {
            sb.append(kv.key()).append(" ");
          }
          sb.append("] ");
        } else {
          InternalNode internal = (InternalNode) node;

          sb.append("{");
          for (Key key : internal.getKeys()) {
            sb.append(key).append(" ");
          }
          sb.append("} ");

          for (Long childId : internal.getChildrenIds()) {
            queue.add(pager.readPage(childId));
          }
        }
      }

      sb.append("\n"); // new level
    }

    log.info(sb.toString());
  }
}
