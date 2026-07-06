package com.duyvu.database;

import com.duyvu.database.command.CreateTableCommand;
import com.duyvu.database.command.InsertCommand;
import com.duyvu.database.command.SelectCommand;
import com.duyvu.database.engine.DatabaseEngine;
import com.duyvu.database.queryparser.QueryParser;
import com.duyvu.database.result.CreateTableResult;
import com.duyvu.database.result.SelectResult;
import com.duyvu.database.schema.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class Main {
  public static void deleteRecursively(Path path) throws IOException {
    if (Files.notExists(path)) return;

    try (Stream<Path> paths = Files.walk(path)) {
      paths
          .sorted(Comparator.reverseOrder()) // delete children first
          .forEach(
              p -> {
                try {
                  Files.delete(p);
                } catch (IOException e) {
                  throw new RuntimeException("Failed to delete: " + p, e);
                }
              });
    }
  }

  static void main() throws IOException {
    SelectCommand selectCommand =
        (SelectCommand)
            QueryParser.parseCommand(
                "SELECT * FROM test WHERE id < int(99960) AND id >= int(99950)");

    System.out.println(selectCommand);

    deleteRecursively(Path.of("./data"));
    CreateTableCommand createTableCommand =
        (CreateTableCommand)
            QueryParser.parseCommand("CREATE TABLE test (id INT 2, name STRING 1)");

    Table table =
        ((CreateTableResult) DatabaseEngine.getInstance().execute(createTableCommand)).table();
    System.out.println(table);
    table = DatabaseEngine.getInstance().readTable("test");
    System.out.println(table);

    Instant start = Instant.now();
    for (int i = 0; i < 10_000_000; i++) {
      if (i % 10000 == 0) {
        log.info("Insert: {}", i);
      }
      InsertCommand insertCommand =
          (InsertCommand)
              QueryParser.parseCommand(
                  "INSERT INTO test (id, name) VALUES (int(%d), string(%s))"
                      .formatted(i, UUID.randomUUID().toString()));
      DatabaseEngine.getInstance().execute(insertCommand);
    }
    Instant end = Instant.now();
    System.out.println("Time: " + Duration.between(start, end));

    start = Instant.now();

    SelectResult selectResult = (SelectResult) DatabaseEngine.getInstance().execute(selectCommand);
    end = Instant.now();
    System.out.println("Time Select: " + Duration.between(start, end));
    System.out.println(selectResult.rows().getFirst());
    System.out.println(selectResult.rows().getLast());
    System.out.println(selectResult.rows().size());
  }
}
