package com.duyvu.database;

import com.duyvu.database.command.CreateTableCommand;
import com.duyvu.database.engine.DatabaseEngine;
import com.duyvu.database.schema.ColumnDefinition;
import com.duyvu.database.schema.Header;
import com.duyvu.database.schema.Table;
import java.util.ArrayList;
import java.util.List;

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
  }
}
