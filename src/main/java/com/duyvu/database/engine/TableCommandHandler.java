package com.duyvu.database.engine;

import com.duyvu.database.command.CreateTableCommand;
import com.duyvu.database.command.InsertCommand;
import com.duyvu.database.command.SelectCommand;
import com.duyvu.database.evaluator.EvaluationContext;
import com.duyvu.database.evaluator.Node;
import com.duyvu.database.exception.DatabaseException;
import com.duyvu.database.exception.ErrorCode;
import com.duyvu.database.reader.HeaderReader;
import com.duyvu.database.reader.RecordsValueReader;
import com.duyvu.database.reader.TypeLengthValueReader;
import com.duyvu.database.result.SelectResult;
import com.duyvu.database.schema.RecordValue;
import com.duyvu.database.schema.RecordsValue;
import com.duyvu.database.schema.Row;
import com.duyvu.database.schema.Table;
import com.duyvu.database.utils.EnvironmentUtils;
import com.duyvu.database.utils.PathUtils;
import lombok.SneakyThrows;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

class TableCommandHandler {
  private Path getTablePath(String tableName) {
    return Paths.get(EnvironmentUtils.getDatabasePath(), tableName + ".bin");
  }

  @SneakyThrows
  void saveTableToFile(Table table) {
    FileHandler.getInstance().addFileHandler(table.getPath());
    RandomAccessFile raf = FileHandler.getInstance().getFileHandler(table.getPath());

    raf.seek(0);
    TypeLengthValueReader reader = new TypeLengthValueReader();
    byte[] data = reader.read(table.getHeader());
    raf.write(data);
  }

  Table createTable(CreateTableCommand createTableCommand) {
    Path tablePath = getTablePath(createTableCommand.getName());

    if (Files.exists(tablePath)) {
      throw new DatabaseException(ErrorCode.TABLE_NAME_ALREADY_EXIST);
    }
    try {
      PathUtils.createFileIfNotExists(tablePath);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create table file", e);
    }
    Table table = new Table(createTableCommand.getHeader(), tablePath);

    saveTableToFile(table);
    return table;
  }

  // TODO: Implement cache for this
  Table getTable(String tableName) {
    Path tablePath = getTablePath(tableName);
    RandomAccessFile raf = FileHandler.getInstance().getFileHandler(tablePath);

    HeaderReader headerReader = new HeaderReader();
    return new Table(headerReader.read(raf), tablePath);
  }

  boolean evaluateExpression(Node whereExpression, Map<String, Object> values) {
    if (whereExpression == null) {
      return true;
    }
    return whereExpression.evaluate(new EvaluationContext(values));
  }

  void insert(InsertCommand insertCommand) {
    Table table = getTable(insertCommand.getTableName());
    Set<String> insertColumnNames = insertCommand.getValues().keySet();
    List<String> columnNames = table.getColumnNames();
    if (!insertColumnNames.containsAll(columnNames)) {
      throw new DatabaseException(ErrorCode.COLUMN_NAMES_NOT_EXISTS);
    }

    List<RecordValue> recordValues = new ArrayList<>();
    for (String columnName : columnNames) {
      Object value = insertCommand.getValues().get(columnName);
      recordValues.add(new RecordValue(value));
    }
    RecordsValue recordsValue = new RecordsValue(recordValues);
    RandomAccessFile raf = FileHandler.getInstance().getFileHandler(table.getPath());
    try {
      raf.seek(raf.length());
      raf.write(new TypeLengthValueReader().read(recordsValue));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SneakyThrows
  public SelectResult select(SelectCommand selectCommand) {
    Table table = getTable(selectCommand.tableName());
    List<String> columnNames = table.getColumnNames();

    List<Row> rows = new ArrayList<>();
    RandomAccessFile raf = FileHandler.getInstance().getFileHandler(table.getPath());
    RecordsValueReader recordsValueReader = new RecordsValueReader();
    while (raf.getFilePointer() < raf.length()) {
      Map<String, Object> values = new HashMap<>();
      RecordsValue recordsValue = recordsValueReader.read(raf);
      for (int i = 0; i < recordsValue.recordValues().size(); i++) {
        String columnName = columnNames.get(i);
        values.put(columnName, recordsValue.recordValues().get(i).getOriginalValue());
      }

      if (!evaluateExpression(selectCommand.whereExpression(), values)) {
        continue;
      }

      rows.add(new Row(values));
    }

    return new SelectResult(selectCommand.tableName(), rows);
  }
}
