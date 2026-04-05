package com.duyvu.database.schema;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import java.util.HashMap;
import java.util.Map;

@Getter
@RequiredArgsConstructor
@Log4j2
public enum Type {
  HEADER((byte) 1),
  COLUMN_DEFINITION((byte) 2),
  STRING((byte) 3),
  INT((byte) 4),
  BYTE((byte) 5),
  RECORD((byte) 6),
  LONG((byte) 7),
  DOUBLE((byte) 8),
  FLOAT((byte) 9),
  DELETED_RECORD((byte) 10),
  PAGE((byte) 127),
  ;

  private final byte code;

  private static final Map<Byte, Type> CODE_MAP;

  static {
    Map<Byte, Type> map = new HashMap<>();
    for (Type type : Type.values()) {
      map.put(type.getCode(), type);
    }
    CODE_MAP = Map.copyOf(map);
  }

  public static Type fromCode(byte code) {
    return CODE_MAP.get(code);
  }
}
