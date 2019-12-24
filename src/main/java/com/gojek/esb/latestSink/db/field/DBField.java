package com.gojek.esb.latestSink.db.field;

public interface DBField {
    Object getColumn() throws RuntimeException;

    boolean canProcess();
}
