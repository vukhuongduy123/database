package com.duyvu.database.command;

import java.util.Map;
import lombok.Builder;

@Builder
public record InsertCommand(String tableName, Map<String, Object> values) implements Command {}
