package com.duyvu.database.queryparser;

import static com.google.common.labs.parse.Parser.*;

import com.duyvu.database.command.SelectCommand;
import com.duyvu.database.evaluator.Node;
import com.duyvu.database.evaluator.OperandNode;
import com.duyvu.database.evaluator.OperatorNode;
import com.duyvu.database.schema.RecordValue;
import com.google.common.labs.parse.Parser;
import java.util.List;

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

  static final Parser<String> VALUE =
      anyOf(quotedByWithEscapes('\'', '\'', chars(1)), consecutive("[^,)]"));

  static final Parser<ValueFunction> FUNCTION =
      consecutive("[a-zA-Z]")
          .suchThat(e -> ValueFunction.fromName(e).isPresent(), "function name")
          .map(
              e ->
                  ValueFunction.fromName(e)
                      .orElseThrow(() -> new IllegalArgumentException("Unknown function: " + e)));

  static final Parser<Node> WHERE_CLAUSE =
      define(
          self -> {
            Parser<Node> node =
                sequence(
                    IDENTIFIER,
                    consecutive(OPERATORS),
                    FUNCTION,
                    VALUE.between("(", ")"),
                    (col, operandSymbol, fn, value) -> {
                      OperandNode.Operand operand = OperandNode.Operand.fromSymbol(operandSymbol);
                      RecordValue recordValue = new RecordValue(fn.convert(value));
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

  public static SelectCommand parseSelectQuery(String query) {
    return SELECT_QUERY.skipping(Character::isWhitespace).parse(query);
  }
}
