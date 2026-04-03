package com.duyvu.database.reader;

public interface Reader<S, D> {
  D read(S data);
}
