package com.gojek.esb.sink.db.field;

public interface DBField {
    Object getColumn() throws RuntimeException;

    boolean canProcess();
}
