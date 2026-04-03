package com.duyvu.database.exception;

public class DatabaseException extends RuntimeException {
	private final ErrorCode errorCode;

	public DatabaseException(ErrorCode errorCode) {
		this.errorCode = errorCode;
	}
}
