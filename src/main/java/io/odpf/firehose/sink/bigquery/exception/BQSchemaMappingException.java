package io.odpf.firehose.sink.bigquery.exception;

public class BQSchemaMappingException extends RuntimeException {
    public BQSchemaMappingException(String message) {
        super(message);
    }
}
