package com.duyvu.database.engine;

import com.duyvu.database.command.CreateTableCommand;
import com.duyvu.database.converter.TypeLengthValueConverter;
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

class TableCommandHandler {
	@SneakyThrows
	void saveTableToFile(Table table) {
		FileHandler.getInstance().addFileHandler(table.getPath());
		RandomAccessFile raf = FileHandler.getInstance().getFileHandler(table.getPath());

		raf.seek(0);
		TypeLengthValueConverter converter = new TypeLengthValueConverter();
		byte[] data = converter.convert(table.getHeader());
		raf.write(data);
	}

	Table createTable(CreateTableCommand createTableCommand) {
		String databasePath = EnvironmentUtils.getDatabasePath();
		Path tablePath = Paths.get(databasePath, createTableCommand.getName() + ".bin");

		if (PathUtils.isFile(tablePath)) {
			throw new IllegalArgumentException("Table path is not a file");
		}

		try {
			if (Files.exists(tablePath)) {
				throw new DatabaseException(ErrorCode.TABLE_NAME_ALREADY_EXIST);
			}
			PathUtils.createFileIfNotExists(tablePath);
		} catch (Exception e) {
			throw new RuntimeException("Failed to create table file", e);
		}
		Table table = new Table(createTableCommand.getHeader(), tablePath);

		saveTableToFile(table);
		return table;
	}
}
