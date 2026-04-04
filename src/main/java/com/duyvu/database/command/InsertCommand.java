package com.duyvu.database.command;

import java.util.Map;

public record InsertCommand(String tableName, Map<String, Object> values) {}
