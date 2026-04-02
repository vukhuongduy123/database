package com.duyvu.database.schema;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@Getter
@RequiredArgsConstructor
public enum Type {
	HEADER((byte) 1),
	COLUMN_DEFINITION((byte) 2),
	STRING((byte) 3),
	INT((byte) 4),
	BYTE((byte) 5);

	private final byte code;

	private final static Map<Byte, Type> CODE_MAP;

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
