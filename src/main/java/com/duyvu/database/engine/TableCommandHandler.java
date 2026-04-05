package com.duyvu.database.engine;

import com.duyvu.database.command.CreateTableCommand;
import com.duyvu.database.command.InsertCommand;
import com.duyvu.database.command.SelectCommand;
import com.duyvu.database.command.UpdateCommand;
import com.duyvu.database.evaluator.EvaluationContext;
import com.duyvu.database.evaluator.Node;
import com.duyvu.database.exception.DatabaseException;
import com.duyvu.database.exception.ErrorCode;
import com.duyvu.database.reader.HeaderReader;
import com.duyvu.database.reader.RecordsValueReader;
import com.duyvu.database.reader.TypeLengthValueReader;
import com.duyvu.database.result.DeleteResult;
import com.duyvu.database.result.SelectResult;
import com.duyvu.database.result.UpdateResult;
import com.duyvu.database.schema.*;
import com.duyvu.database.utils.EnvironmentUtils;
import com.duyvu.database.utils.LRUCache;
import com.duyvu.database.utils.PathUtils;
import lombok.SneakyThrows;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static com.duyvu.database.utils.Constants.UNKNOWN_OFFSET;

class TableCommandHandler {
  private final LRUCache<String, Table> tableCache = new LRUCache<>(100);

  private Path getTablePath(String tableName) {
    return Paths.get(EnvironmentUtils.getDatabasePath(), tableName + ".bin");
  }

  @SneakyThrows
  Table saveTableToFile(Path tablePath, Header header) {
    FileHandler.getInstance().addFileHandler(tablePath);
    RandomAccessFile raf = FileHandler.getInstance().getFileHandler(tablePath);

    Table table = new Table(header, tablePath, 0);

    raf.seek(0);
    TypeLengthValueReader reader = new TypeLengthValueReader();
    byte[] data = reader.read(table.getHeader());
    raf.write(data);
    table.setOffset(raf.getFilePointer());
    return table;
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

    return saveTableToFile(tablePath, createTableCommand.getHeader());
  }

  @SneakyThrows
  Table getTable(String tableName) {
    if (!tableCache.containsKey(tableName)) {
      Path tablePath = getTablePath(tableName);
      RandomAccessFile raf = FileHandler.getInstance().getFileHandler(tablePath);

      HeaderReader headerReader = new HeaderReader();
      Table table = new Table(headerReader.read(raf), tablePath, raf.getFilePointer());
      tableCache.put(tableName, table);
    }

    Table table = tableCache.get(tableName);
    RandomAccessFile raf = FileHandler.getInstance().getFileHandler(table.getPath());
    raf.seek(table.getOffset());
    return table;
  }

  boolean evaluateExpression(Node whereExpression, Map<String, Object> values) {
    if (whereExpression == null) {
      return true;
    }
    return whereExpression.evaluate(new EvaluationContext(values));
  }

  void insert(InsertCommand insertCommand) {
    Table table = getTable(insertCommand.tableName());
    Set<String> insertColumnNames = insertCommand.values().keySet();
    List<String> columnNames = table.getColumnNames();
    if (!insertColumnNames.containsAll(columnNames)) {
      throw new DatabaseException(ErrorCode.COLUMN_NAMES_NOT_EXISTS);
    }

    List<RecordValue> recordValues = new ArrayList<>();
    Map<String, ColumnDefinition> columnDefinitionMap = table.getHeader().getColumnDefinitionMap();
    for (String columnName : columnNames) {
      Object value = insertCommand.values().get(columnName);
      RecordValue recordValue = new RecordValue(value);
      if (recordValue.getType()
          != Type.fromCode(columnDefinitionMap.get(columnName).columnType().getValue()[0])) {
        throw new DatabaseException(ErrorCode.INVALID_VALUE_TYPE);
      }
      recordValues.add(recordValue);
    }
    RecordsValue recordsValue = new RecordsValue(Type.RECORD, recordValues, UNKNOWN_OFFSET);
    RandomAccessFile raf = FileHandler.getInstance().getFileHandler(table.getPath());
    try {
      raf.seek(raf.length());
      raf.write(new TypeLengthValueReader().read(recordsValue));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SneakyThrows
  SelectResult select(SelectCommand selectCommand) {
    Table table = getTable(selectCommand.tableName());
    List<String> columnNames = table.getColumnNames();

    List<Row> rows = new ArrayList<>();
    RandomAccessFile raf = FileHandler.getInstance().getFileHandler(table.getPath());
    RecordsValueReader recordsValueReader = new RecordsValueReader();
    while (raf.getFilePointer() < raf.length()) {
      Map<String, Object> values = new HashMap<>();
      RecordsValue recordsValue = recordsValueReader.read(raf);
      if (recordsValue.type() == Type.DELETED_RECORD) {
        continue;
      }
      for (int i = 0; i < recordsValue.recordValues().size(); i++) {
        String columnName = columnNames.get(i);
        values.put(columnName, recordsValue.recordValues().get(i).getOriginalValue());
      }

      if (!evaluateExpression(selectCommand.whereExpression(), values)) {
        continue;
      }

      rows.add(new Row(values, recordsValue.offset()));
      if (rows.size() >= selectCommand.limit()) {
        break;
      }
    }

    return new SelectResult(selectCommand.tableName(), rows);
  }

  @SneakyThrows
  DeleteResult delete(SelectCommand selectCommand) {
    SelectResult selectResult = select(selectCommand);
    deleteRows(selectCommand.tableName(), selectResult.rows());

    return new DeleteResult(selectCommand.tableName(), selectResult.rows().size());
  }

  void deleteRows(String tableName, List<Row> rows) throws IOException {
    RandomAccessFile raf = FileHandler.getInstance().getFileHandler(getTable(tableName).getPath());
    for (Row row : rows) {
      long rowOffset = row.getOffset();
      if (rowOffset == UNKNOWN_OFFSET) {
        throw new DatabaseException(ErrorCode.UNKNOWN_OFFSET);
      }

      raf.seek(rowOffset);
      raf.writeByte(Type.DELETED_RECORD.getCode());
    }
  }

  @SneakyThrows
  UpdateResult update(UpdateCommand updateCommand) {
    SelectResult selectResult =
        select(new SelectCommand(updateCommand.tableName(), updateCommand.whereExpression()));

    deleteRows(updateCommand.tableName(), selectResult.rows());

    for (Row row : selectResult.rows()) {
      Map<String, Object> values = new HashMap<>();
      values.putAll(row.getValues());
      values.putAll(updateCommand.newValues());

      InsertCommand insertCommand = new InsertCommand(updateCommand.tableName(), values);
      insert(insertCommand);
    }

    return new UpdateResult(updateCommand.tableName(), selectResult.rows().size());
  }
}
