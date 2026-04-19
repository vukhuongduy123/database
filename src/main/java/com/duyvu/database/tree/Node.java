package com.duyvu.database.tree;

import com.duyvu.database.schema.Type;
import com.duyvu.database.schema.TypeLengthValue;
import lombok.Getter;
import lombok.Setter;

public abstract class Node implements TypeLengthValue {
  Node(long pageId) {
    this.pageId = pageId;
  }

  public abstract Type getType();

  @Getter @Setter protected long pageId;
}
