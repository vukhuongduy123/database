package com.duyvu.database;

import com.duyvu.database.command.CreateTableCommand;
import com.duyvu.database.command.InsertCommand;
import com.duyvu.database.command.SelectCommand;
import com.duyvu.database.engine.DatabaseEngine;
import com.duyvu.database.evaluator.Node;
import com.duyvu.database.evaluator.OperandNode;
import com.duyvu.database.schema.ColumnDefinition;
import com.duyvu.database.schema.Header;
import com.duyvu.database.schema.RecordValue;
import com.duyvu.database.schema.Table;
import lombok.extern.log4j.Log4j2;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Log4j2
public class Main {
  static void main() {
    List<ColumnDefinition> columnDefinitions = new ArrayList<>();
    {
      ColumnDefinition columnDefinition =
          new ColumnDefinition(
              new ColumnDefinition.ColumnName("name"),
              new ColumnDefinition.ColumnType(ColumnDefinition.ColumnType.STRING),
              new ColumnDefinition.ColumnAttribute(
                  new byte[] {ColumnDefinition.ColumnAttribute.NULLABLE}));
      columnDefinitions.add(columnDefinition);
    }

    {
      ColumnDefinition columnDefinition =
          new ColumnDefinition(
              new ColumnDefinition.ColumnName("id"),
              new ColumnDefinition.ColumnType(ColumnDefinition.ColumnType.INT),
              new ColumnDefinition.ColumnAttribute(
                  new byte[] {ColumnDefinition.ColumnAttribute.PRIMARY_KEY}));
      columnDefinitions.add(columnDefinition);
    }

    Header header = new Header(columnDefinitions);
    CreateTableCommand createTableCommand =
        CreateTableCommand.builder().name("test").header(header).build();
    Table table = DatabaseEngine.getInstance().createTable(createTableCommand);
    System.out.println(table);
    table = DatabaseEngine.getInstance().readTable("test");
    System.out.println(table);

    Instant start = Instant.now();
    for (int i = 0; i < 1000_000; i++) {
      log.info("Insert {}", i);
      InsertCommand insertCommand = new InsertCommand("test", Map.of("id", i, "name", "test"));
      DatabaseEngine.getInstance().insert(insertCommand);
    }
    Instant end = Instant.now();
    System.out.println("Time: " + Duration.between(start, end));

    Node whereClause = new OperandNode("id", OperandNode.Operand.GTE, new RecordValue(999500));
    
    DatabaseEngine.getInstance().select(new SelectCommand("test", whereClause));
  }
}
