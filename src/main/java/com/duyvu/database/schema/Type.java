package com.duyvu.database.schema;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
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
  INTERNAL_NODE((byte) 11),
  LEAF_NODE((byte) 12),
  KEY((byte) 13),
  VALUE((byte) 14),
  KEY_VALUE((byte) 15);

  private final byte code;

  private static final Map<Byte, Type> CODE_MAP;

  private static final Map<String, Type> COLUM_TYPE_NAME_MAP =
      Map.of(
          INT.name(), INT,
          STRING.name(), STRING,
          LONG.name(), LONG,
          DOUBLE.name(), DOUBLE,
          FLOAT.name(), FLOAT);

  static {
    Map<Byte, Type> map = new HashMap<>();
    for (Type type : Type.values()) {
      map.put(type.getCode(), type);
    }
    CODE_MAP = Map.copyOf(map);
  }

  public static Type fromCode(byte code) {
    if (!CODE_MAP.containsKey(code)) {
      throw new IllegalArgumentException("Invalid type code " + code);
    }
    return CODE_MAP.get(code);
  }

  public static Optional<Type> fromColumTypeName(String name) {
    return Optional.ofNullable(COLUM_TYPE_NAME_MAP.get(name));
  }
}
