package com.duyvu.database.schema;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Row {
  private Map<String, Object> values;

  private long offset;
}
