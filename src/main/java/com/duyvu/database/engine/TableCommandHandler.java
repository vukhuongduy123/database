package com.duyvu.database.engine;

import com.duyvu.database.command.CreateTableCommand;
import com.duyvu.database.command.InsertCommand;
import com.duyvu.database.converter.HeaderReader;
import com.duyvu.database.converter.TypeLengthValueReader;
import com.duyvu.database.exception.DatabaseException;
import com.duyvu.database.exception.ErrorCode;
import com.duyvu.database.schema.Table;
import com.duyvu.database.utils.EnvironmentUtils;
import com.duyvu.database.utils.PathUtils;
import lombok.SneakyThrows;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

class TableCommandHandler {
	private Path getTablePath(String tableName) {
		return Paths.get(EnvironmentUtils.getDatabasePath(), tableName + ".bin");
	}

	@SneakyThrows
	void saveTableToFile(Table table) {
		FileHandler.getInstance().addFileHandler(table.getPath());
		RandomAccessFile raf = FileHandler.getInstance().getFileHandler(table.getPath());

		raf.seek(0);
		TypeLengthValueReader converter = new TypeLengthValueReader();
		byte[] data = converter.read(table.getHeader());
		raf.write(data);
	}

	Table createTable(CreateTableCommand createTableCommand) {
		Path tablePath = getTablePath(createTableCommand.getName());

		if (Files.exists(tablePath)) {
			throw new DatabaseException(ErrorCode.TABLE_NAME_ALREADY_EXIST);
		}
		try {
			PathUtils.createFileIfNotExists(tablePath);
		} catch (Exception e) {
			throw new RuntimeException("Failed to create table file", e);
		}
		Table table = new Table(createTableCommand.getHeader(), tablePath);

		saveTableToFile(table);
		return table;
	}

	Table getTable(String tableName) {
		Path tablePath = getTablePath(tableName);
		RandomAccessFile raf = FileHandler.getInstance().getFileHandler(tablePath);

		HeaderReader headerReader = new HeaderReader();
		return new Table(headerReader.read(raf), tablePath);
	}

	void insert(InsertCommand insertCommand) {
		Table table = getTable(insertCommand.getTableName());
		Set<String> insertColumnNames = insertCommand.getValues().keySet();
		Set<String> columnNames =
				table.getHeader().columnDefinitions().stream()
						.map(cd -> cd.columnName().getName())
						.collect(toSet());
	}
}
