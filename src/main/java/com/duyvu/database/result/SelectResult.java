package com.duyvu.database.result;

import com.duyvu.database.schema.Row;

import java.util.List;

public record SelectResult(String tableName, List<Row> rows) {
}