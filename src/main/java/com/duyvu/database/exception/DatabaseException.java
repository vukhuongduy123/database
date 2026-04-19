package com.duyvu.database.exception;

import lombok.Getter;

@Getter
public class DatabaseException extends RuntimeException {
  private final ErrorCode errorCode;

  public DatabaseException(ErrorCode errorCode) {
    this.errorCode = errorCode;
  }

  public DatabaseException(ErrorCode errorCode, Throwable e) {
    super(e);
    this.errorCode = errorCode;
  }
}
