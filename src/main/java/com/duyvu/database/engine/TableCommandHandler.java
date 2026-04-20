package com.duyvu.database.engine;

import com.duyvu.database.command.CreateTableCommand;
import com.duyvu.database.command.InsertCommand;
import com.duyvu.database.command.SelectCommand;
import com.duyvu.database.command.UpdateCommand;
import com.duyvu.database.evaluator.EvaluationContext;
import com.duyvu.database.evaluator.Node;
import com.duyvu.database.evaluator.OperandNode;
import com.duyvu.database.evaluator.OperatorNode;
import com.duyvu.database.exception.DatabaseException;
import com.duyvu.database.exception.ErrorCode;
import com.duyvu.database.reader.HeaderReader;
import com.duyvu.database.reader.RecordsValueReader;
import com.duyvu.database.reader.TypeLengthValueReader;
import com.duyvu.database.result.DeleteResult;
import com.duyvu.database.result.SelectResult;
import com.duyvu.database.result.UpdateResult;
import com.duyvu.database.schema.*;
import com.duyvu.database.tree.Key;
import com.duyvu.database.tree.KeyValue;
import com.duyvu.database.tree.Tree;
import com.duyvu.database.tree.Value;
import com.duyvu.database.utils.EnvironmentUtils;
import com.duyvu.database.utils.FileHandler;
import com.duyvu.database.utils.LRUCache;
import com.duyvu.database.utils.PathUtils;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static com.duyvu.database.utils.Constants.UNKNOWN_OFFSET;

@Log4j2
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

  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  boolean evaluateExpression(Node whereExpression, Map<String, Object> values) {
    if (whereExpression == null) {
      return true;
    }
    return whereExpression.evaluate(new EvaluationContext(values));
  }

  OperandNode getOperandNodeForVariable(Node whereExpression, String variable) {
    if (whereExpression == null) {
      return null;
    }
    return whereExpression.getOperand(variable);
  }

  void insert(InsertCommand insertCommand) {
    Table table = getTable(insertCommand.tableName());
    Set<String> insertColumnNames = insertCommand.values().keySet();
    List<String> columnNames = table.getColumnNames();
    if (!insertColumnNames.containsAll(columnNames)) {
      throw new DatabaseException(ErrorCode.COLUMN_NAMES_NOT_EXISTS);
    }

    List<RecordValue> recordValues = new ArrayList<>();
    Map<String, RecordValue> recordValueMap = new HashMap<>();
    Map<String, ColumnDefinition> columnDefinitionMap = table.getHeader().getColumnDefinitionMap();
    for (String columnName : columnNames) {
      Object value = insertCommand.values().get(columnName);
      RecordValue recordValue = new RecordValue(value);
      if (recordValue.getType()
          != Type.fromCode(columnDefinitionMap.get(columnName).columnType().getValue()[0])) {
        throw new DatabaseException(ErrorCode.INVALID_VALUE_TYPE);
      }
      recordValues.add(recordValue);
      recordValueMap.put(columnName, recordValue);
    }

    RecordsValue recordsValue = new RecordsValue(Type.RECORD, recordValues, UNKNOWN_OFFSET);
    RandomAccessFile raf = FileHandler.getInstance().getFileHandler(table.getPath());
    try {
      raf.seek(raf.length());
      long rowOffset = raf.getFilePointer();
      raf.write(new TypeLengthValueReader().read(recordsValue));

      Map<String, Tree> indexMap = table.getHeader().getIndexMap();
      for (ColumnDefinition columnDefinition : columnDefinitionMap.values()) {
        if (columnDefinition.columnAttribute().isIndex()) {
          RecordValue recordValue = recordValueMap.get(columnDefinition.columnName().getName());
          Tree indexTree = indexMap.get(columnDefinition.columnName().getName());
          indexTree.insert(
              new Key(ByteBuffer.wrap(recordValue.getValue())),
              new Value(ByteBuffer.allocate(Long.BYTES).putLong(rowOffset)));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (Exception e) {
      throw new DatabaseException(ErrorCode.INSERTION_ERROR, e);
    }
  }

  @SneakyThrows
  SelectResult select(SelectCommand selectCommand) {
    Table table = getTable(selectCommand.tableName());
    List<String> columnNames = table.getColumnNames();

    Set<String> indexColumnNames = table.getHeader().getIndexMap().keySet();
    if (indexColumnNames.stream().anyMatch(columnNames::contains)) {
      String indexColumnName =
          indexColumnNames.stream().filter(columnNames::contains).findFirst().orElseThrow();
      return selectUsingIndex(table, columnNames, indexColumnName, selectCommand);
    }

    return selectNoIndex(table, columnNames, selectCommand);
  }

  private SelectResult selectUsingIndex(
      Table table, List<String> columnNames, String indexColumnName, SelectCommand selectCommand)
      throws IOException {
    List<Row> rows = new ArrayList<>();
    RandomAccessFile raf = FileHandler.getInstance().getFileHandler(table.getPath());
    RecordsValueReader recordsValueReader = new RecordsValueReader();
    OperandNode operandNode =
        getOperandNodeForVariable(selectCommand.whereExpression(), indexColumnName);

    if (operandNode == null) {
      log.debug(
          "Index column {} is not used in where expression, fallback to full scan",
          indexColumnName);
      return selectNoIndex(table, columnNames, selectCommand);
    }

    if (selectCommand.whereExpression().containsOperator(OperatorNode.Operator.OR)) {
      log.debug(
          "Operator {} is used in where expression, fallback to full scan", OperatorNode.Operator.OR);
      return selectNoIndex(table, columnNames, selectCommand);
    }

    Tree indexTree = table.getHeader().getIndexMap().get(indexColumnName);
    List<KeyValue> keyValues = new ArrayList<>();
    Key searchKey = new Key(ByteBuffer.wrap(operandNode.recordValue().getValue()));
    switch (operandNode.operand()) {
      case EQ:
        {
          keyValues.add(indexTree.search(searchKey));
          break;
        }
      case GT:
        {
          keyValues.addAll(indexTree.greaterThan(searchKey));
          break;
        }
      case GTE:
        {
          keyValues.addAll(indexTree.greaterThanOrEqual(searchKey));
        }
      case LT:
        {
          keyValues.addAll(indexTree.lessThan(searchKey));
          break;
        }
      case LTE:
        {
          keyValues.addAll(indexTree.lessThanOrEqual(searchKey));
          break;
        }
      case NEQ:
        {
          keyValues.addAll(indexTree.lessThan(searchKey));
          keyValues.addAll(indexTree.greaterThan(searchKey));
          break;
        }
      case null, default:
        {
          log.debug(
              "Unsupported operand {} for index column {}, fallback to full scan",
              operandNode.operand(),
              indexColumnName);
          return selectNoIndex(table, columnNames, selectCommand);
        }
    }

    for (KeyValue keyValue : keyValues) {
      Value value = keyValue.value();
      ByteBuffer buffer = ByteBuffer.wrap(value.getValue());
      long rowOffset = buffer.getLong();

      raf.seek(rowOffset);
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

  private SelectResult selectNoIndex(
      Table table, List<String> columnNames, SelectCommand selectCommand) throws IOException {
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
    deleteIndexRows(selectCommand.tableName(), selectResult.rows());

    return new DeleteResult(selectCommand.tableName(), selectResult.rows().size());
  }

  private void deleteIndexRows(String tableName, List<Row> rows) {
    Table table = getTable(tableName);
    Map<String, Tree> indexMap = table.getHeader().getIndexMap();
    Map<String, ColumnDefinition> columnDefinitionMap = table.getHeader().getColumnDefinitionMap();

    for (Row row : rows) {
      for (ColumnDefinition columnDefinition : columnDefinitionMap.values()) {
        if (columnDefinition.columnAttribute().isIndex()) {
          Tree indexTree = indexMap.get(columnDefinition.columnName().getName());
          Object value = row.getValues().get(columnDefinition.columnName().getName());
          RecordValue recordValue = new RecordValue(value);
          indexTree.delete(new Key(ByteBuffer.wrap(recordValue.getValue())));
        }
      }
    }
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
