package com.duyvu.database;

import com.duyvu.database.converter.HeaderConverter;
import com.duyvu.database.converter.TypeLengthValueConverter;
import com.duyvu.database.schema.ColumnDefinition;
import com.duyvu.database.schema.Header;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
	static void main() {
		List<ColumnDefinition> columnDefinitions = new ArrayList<>();
		{
			ColumnDefinition columnDefinition = new ColumnDefinition(
					new ColumnDefinition.ColumnName("name"),
					new ColumnDefinition.ColumnType(ColumnDefinition.ColumnType.STRING),
					new ColumnDefinition.ColumnAttribute(new byte[]{ColumnDefinition.ColumnAttribute.NULLABLE})
			);
			columnDefinitions.add(columnDefinition);
		}

		{
			ColumnDefinition columnDefinition = new ColumnDefinition(
					new ColumnDefinition.ColumnName("id"),
					new ColumnDefinition.ColumnType(ColumnDefinition.ColumnType.INT),
					new ColumnDefinition.ColumnAttribute(new byte[]{ColumnDefinition.ColumnAttribute.PRIMARY_KEY})
			);
			columnDefinitions.add(columnDefinition);
		}

		Header header = new Header(columnDefinitions);
		TypeLengthValueConverter converter = new TypeLengthValueConverter();
		System.out.println(Arrays.toString(converter.convert(header)));

		HeaderConverter headerConverter = new HeaderConverter();
		System.out.println(headerConverter.convert(converter.convert(header)));
	}
}
