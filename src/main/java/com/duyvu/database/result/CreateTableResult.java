package com.duyvu.database.result;

import com.duyvu.database.schema.Table;

public record CreateTableResult(Table table) implements QueryResult {}
