package com.duyvu.database.schema;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import java.nio.file.Path;

@Data
@AllArgsConstructor
@Builder
@ToString
public class Table {
	private Header header;
	private Path path;
}
