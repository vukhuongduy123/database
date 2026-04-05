package com.duyvu.database.schema;

import static com.duyvu.database.utils.Constants.META_DATA_LENGTH;

public record PageOffsetLength(long offset, int length) {
  public int getFullLength() {
    return length + META_DATA_LENGTH;
  }
}
