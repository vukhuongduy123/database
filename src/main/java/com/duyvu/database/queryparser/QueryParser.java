package com.duyvu.database.queryparser;

import static com.google.common.labs.parse.Parser.*;

import com.duyvu.database.command.*;
import com.duyvu.database.evaluator.Node;
import com.duyvu.database.evaluator.OperandNode;
import com.duyvu.database.evaluator.OperatorNode;
import com.duyvu.database.schema.ColumnDefinition;
import com.duyvu.database.schema.Header;
import com.duyvu.database.schema.RecordValue;
import com.duyvu.database.schema.Type;
import com.google.common.labs.parse.Parser;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class QueryParser {
  private QueryParser() {
    throw new IllegalStateException("Utility class");
  }

  static final String OPERATORS = "[" + OperandNode.Operand.getAllSymbols() + "]";

  static Parser<String> insensitiveKeyword(String keyword) {
    return consecutive("[a-zA-Z]").suchThat(e -> e.equalsIgnoreCase(keyword), keyword);
  }

  static final Parser<String> IDENTIFIER = consecutive("[a-zA-Z_][a-zA-Z0-9_]");

  static final Parser<List<String>> COLUMN_NAMES =
      anyOf(string("*").thenReturn(List.of("*")), IDENTIFIER.atLeastOnceDelimitedBy(","));

  static final Parser<List<String>> COLUMN_NAMES_NO_WILDCARD =
      IDENTIFIER.atLeastOnceDelimitedBy(",");

  static final Parser<String> VALUE =
      anyOf(quotedByWithEscapes('\'', '\'', chars(1)), consecutive("[^,)]"));

  static final Parser<ValueFunction> FUNCTION =
      consecutive("[a-zA-Z]")
          .suchThat(e -> ValueFunction.fromName(e).isPresent(), "function name")
          .map(
              e ->
                  ValueFunction.fromName(e)
                      .orElseThrow(() -> new IllegalArgumentException("Unknown function: " + e)));

  static final Parser<Object> COLUMN_VALUE =
      sequence(FUNCTION, VALUE.between("(", ")"), ValueFunction::convert);

  static final Parser<Node> WHERE_CLAUSE =
      define(
          self -> {
            Parser<Node> node =
                sequence(
                    IDENTIFIER,
                    consecutive(OPERATORS),
                    COLUMN_VALUE,
                    (col, operandSymbol, value) -> {
                      OperandNode.Operand operand = OperandNode.Operand.fromSymbol(operandSymbol);
                      RecordValue recordValue = new RecordValue(value);
                      return new OperandNode(col, operand, recordValue);
                    });

            Parser<Node> atom = anyOf(node, self.between("(", ")"));

            Parser<Node> andNode =
                atom.withPostfixes(
                    insensitiveKeyword(OperatorNode.Operator.AND.name()).then(atom),
                    (left, right) -> new OperatorNode(OperatorNode.Operator.AND, left, right));

            return andNode.withPostfixes(
                insensitiveKeyword(OperatorNode.Operator.OR.name()).then(andNode),
                (left, right) -> new OperatorNode(OperatorNode.Operator.OR, left, right));
          });

  static final Parser<Long> LIMIT = digits().map(Long::parseLong).suchThat(e -> e > 0, "limit");

  static final Parser<Map<String, Object>> SET_CLAUSE =
      sequence(
              IDENTIFIER,
              insensitiveKeyword("="),
              COLUMN_VALUE,
              (col, _, value) -> Map.entry(col, value))
          .atLeastOnceDelimitedBy(",")
          .map(
              pairs -> {
                Map<String, Object> map = new HashMap<>(pairs.size());
                for (Map.Entry<String, Object> pair : pairs) {
                  map.put(pair.getKey(), pair.getValue());
                }
                return map;
              });

  static final Parser<SelectCommand> SELECT_QUERY =
      sequence(
              insensitiveKeyword("SELECT").then(COLUMN_NAMES),
              insensitiveKeyword("FROM").then(IDENTIFIER),
              (columns, table) -> SelectCommand.builder().tableName(table).columnNames(columns))
          .optionallyFollowedBy(
              insensitiveKeyword("WHERE").then(WHERE_CLAUSE),
              SelectCommand.SelectCommandBuilder::whereExpression)
          .optionallyFollowedBy(
              insensitiveKeyword("LIMIT").then(LIMIT), SelectCommand.SelectCommandBuilder::limit)
          .map(SelectCommand.SelectCommandBuilder::build);

  static final Parser<SelectCommand> DELETE_QUERY =
      insensitiveKeyword("DELETE")
          .then(insensitiveKeyword("FROM"))
          .then(IDENTIFIER)
          .map(table -> SelectCommand.builder().tableName(table).columnNames(List.of()))
          .optionallyFollowedBy(
              insensitiveKeyword("WHERE").then(WHERE_CLAUSE),
              SelectCommand.SelectCommandBuilder::whereExpression)
          .map(SelectCommand.SelectCommandBuilder::build);

  static final Parser<UpdateCommand> UPDATE_QUERY =
      sequence(
          insensitiveKeyword("UPDATE").then(insensitiveKeyword("TABLE")).then(IDENTIFIER),
          insensitiveKeyword("SET").then(SET_CLAUSE),
          insensitiveKeyword("WHERE").then(WHERE_CLAUSE).optional(),
          (table, setClause, whereClause) ->
              UpdateCommand.builder()
                  .tableName(table)
                  .newValues(setClause)
                  .whereExpression(whereClause.orElse(null))
                  .build());

  static final Parser<ColumnDefinition.ColumnType> COLUMN_TYPE =
      consecutive("[a-zA-Z]")
          .map(
              e ->
                  Type.fromColumTypeName(e)
                      .orElseThrow(() -> new IllegalArgumentException("Invalid column type: " + e)))
          .map(ColumnDefinition.ColumnType::new);

  static final Parser<ColumnDefinition.ColumnAttribute> COLUMN_ATTRIBUTE =
      digits().map(e -> new ColumnDefinition.ColumnAttribute(Integer.parseInt(e)));

  static final Parser<Header> HEADER =
      sequence(
              IDENTIFIER,
              COLUMN_TYPE,
              COLUMN_ATTRIBUTE,
              (name, type, attribute) ->
                  new ColumnDefinition(new ColumnDefinition.ColumnName(name), type, attribute))
          .atLeastOnceDelimitedBy(",")
          .map(Header::new);

  static final Parser<CreateTableCommand> CREATE_TABLE_QUERY =
      sequence(
          insensitiveKeyword("CREATE").then(insensitiveKeyword("TABLE")).then(IDENTIFIER),
          HEADER.between("(", ")"),
          (table, header) -> CreateTableCommand.builder().name(table).header(header).build());

  static final Parser<List<Object>> COLUMN_VALUES = COLUMN_VALUE.atLeastOnceDelimitedBy(",");

  static final Parser<InsertCommand> INSERT_QUERY =
      sequence(
          insensitiveKeyword("INSERT").then(insensitiveKeyword("INTO")).then(IDENTIFIER),
          COLUMN_NAMES_NO_WILDCARD.between("(", ")"),
          insensitiveKeyword("VALUES"),
          COLUMN_VALUES.between("(", ")"),
          (table, columnNames, _, columnValues) -> {
            Map<String, Object> values = new HashMap<>();
            if (columnNames.size() != columnValues.size()) {
              throw new IllegalArgumentException("Invalid number of values");
            }

            for (int i = 0; i < columnNames.size(); i++) {
              values.put(columnNames.get(i), columnValues.get(i));
            }

            return InsertCommand.builder().tableName(table).values(values).build();
          });

  public static Command parseCommand(String query) {
    if (INSERT_QUERY.skipping(Character::isWhitespace).matches(query)) {
      return INSERT_QUERY.skipping(Character::isWhitespace).parse(query);
    } else if (SELECT_QUERY.skipping(Character::isWhitespace).matches(query)) {
      return SELECT_QUERY.skipping(Character::isWhitespace).parse(query);
    } else if (DELETE_QUERY.skipping(Character::isWhitespace).matches(query)) {
      return DELETE_QUERY.skipping(Character::isWhitespace).parse(query);
    } else if (UPDATE_QUERY.skipping(Character::isWhitespace).matches(query)) {
      return UPDATE_QUERY.skipping(Character::isWhitespace).parse(query);
    } else if (CREATE_TABLE_QUERY.skipping(Character::isWhitespace).matches(query)) {
      return CREATE_TABLE_QUERY.skipping(Character::isWhitespace).parse(query);
    }

    throw new IllegalArgumentException("Invalid query: " + query);
  }
}
