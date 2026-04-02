package com.duyvu.database.converter;

public interface Converter<S, D> {
  D convert(S data);
}
