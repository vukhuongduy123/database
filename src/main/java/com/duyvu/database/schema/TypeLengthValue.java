package com.duyvu.database.schema;

import static com.duyvu.database.utils.Constants.META_DATA_LENGTH;

public interface TypeLengthValue {

  Type getType();

  default int getLength() {
    return getValue().length;
  }

  byte[] getValue();
  
  default int getFullLength() {
    return getLength() + META_DATA_LENGTH;
  }
}
