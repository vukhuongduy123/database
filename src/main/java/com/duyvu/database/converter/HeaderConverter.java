package com.duyvu.database.converter;

import com.duyvu.database.schema.ColumnDefinition;
import com.duyvu.database.schema.Header;
import com.duyvu.database.schema.Type;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class HeaderConverter implements Converter<byte[], Header> {
	
	static class ColumnDefinitionConverter implements Converter<byte[], ColumnDefinition> {
		@Override
		public ColumnDefinition convert(byte[] data) {
			ByteBuffer buffer = ByteBuffer.wrap(data);
			
			Type nameType = Type.fromCode(buffer.get());
			if (nameType != Type.STRING) {
				throw new IllegalArgumentException("Invalid type");
			}
			int nameLength = buffer.getInt();
			byte[] nameValue = new byte[nameLength];
			buffer.get(nameValue);
			ColumnDefinition.ColumnName columnName = new ColumnDefinition.ColumnName(new String(nameValue));

			Type typeType = Type.fromCode(buffer.get());
			if (typeType != Type.BYTE) {
				throw new IllegalArgumentException("Invalid type");
			}
			int typeLength = buffer.getInt();
			byte[] typeValue = new byte[typeLength];
			buffer.get(typeValue);
			ColumnDefinition.ColumnType columnType = new ColumnDefinition.ColumnType(typeValue[0]);

			Type attributeType = Type.fromCode(buffer.get());
			if (attributeType != Type.INT) {
				throw new IllegalArgumentException("Invalid type");
			}
			// Skip length as fixed length
			buffer.getInt();
			int attribute = buffer.getInt();
			ColumnDefinition.ColumnAttribute columnAttribute = new ColumnDefinition.ColumnAttribute(attribute);

			return new ColumnDefinition(columnName, columnType, columnAttribute);
		}
	}
	

	@Override
	public Header convert(byte[] data) {
		ByteBuffer buffer = ByteBuffer.wrap(data);
		Type type = Type.fromCode(buffer.get());
		if (type != Type.HEADER) {
			throw new IllegalArgumentException("Invalid type");
		}
		// Skip length
		buffer.getInt();
		ByteBuffer headerBuffer = buffer.slice();
		
		List<ColumnDefinition> columnDefinitions = new ArrayList<>();
		ColumnDefinitionConverter converter = new ColumnDefinitionConverter();
		while (headerBuffer.hasRemaining()) {
			Type columnType = Type.fromCode(headerBuffer.get());
			if (columnType != Type.COLUMN_DEFINITION) {
				throw new IllegalArgumentException("Invalid column definition type");
			}
			int columnLength = headerBuffer.getInt();
			byte[] columnValue = new byte[columnLength];
			headerBuffer.get(columnValue);

			columnDefinitions.add(converter.convert(columnValue));
		}
		
		return new Header(columnDefinitions);
	}
}
