package com.gojek.esb.sink.jdbc.field;

public interface JdbcField {
    Object getColumn() throws RuntimeException;

    boolean canProcess();
}
