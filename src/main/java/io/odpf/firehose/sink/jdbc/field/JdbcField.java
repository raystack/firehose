package io.odpf.firehose.sink.jdbc.field;

public interface JdbcField {
    Object getColumn() throws RuntimeException;

    boolean canProcess();
}
