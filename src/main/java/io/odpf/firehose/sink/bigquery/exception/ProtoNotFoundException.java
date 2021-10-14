package io.odpf.firehose.sink.bigquery.exception;

public class ProtoNotFoundException extends RuntimeException {
    public ProtoNotFoundException(String message) {
        super(message);
    }
}
