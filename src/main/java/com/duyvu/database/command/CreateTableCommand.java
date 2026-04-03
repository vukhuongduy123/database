package com.duyvu.database.command;

import com.duyvu.database.schema.Header;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class CreateTableCommand {
	private String name;
	private Header header;
}
