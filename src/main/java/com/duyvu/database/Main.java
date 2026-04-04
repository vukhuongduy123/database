package com.duyvu.database;

import com.duyvu.database.command.CreateTableCommand;
import com.duyvu.database.command.InsertCommand;
import com.duyvu.database.command.SelectCommand;
import com.duyvu.database.engine.DatabaseEngine;
import com.duyvu.database.result.SelectResult;
import com.duyvu.database.schema.ColumnDefinition;
import com.duyvu.database.schema.Header;
import com.duyvu.database.schema.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    {
      InsertCommand insertCommand = new InsertCommand("test", Map.of("id", 1, "name", "test"));
      DatabaseEngine.getInstance().insert(insertCommand);
    }

    {
      InsertCommand insertCommand = new InsertCommand("test", Map.of("id", 2, "name", "test2"));
      DatabaseEngine.getInstance().insert(insertCommand);
    }

    SelectResult selectResult = DatabaseEngine.getInstance().select(new SelectCommand("test"));
    System.out.println(selectResult);
  }
}
