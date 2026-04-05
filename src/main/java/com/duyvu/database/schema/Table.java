package com.duyvu.database.schema;

import static com.duyvu.database.utils.PathUtils.getFileNameWithoutExtension;

import java.nio.file.Path;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@Builder
@ToString
public class Table {
  private Header header;
  private Path path;
  private long offset;

  public String getName() {
    return getFileNameWithoutExtension(path);
  }

  public List<String> getColumnNames() {
    return header.getColumnDefinitions().stream().map(e -> e.columnName().getName()).toList();
  }
}
