package com.duyvu.database.converter;

public interface Reader<S, D> {
  D read(S data);
}
