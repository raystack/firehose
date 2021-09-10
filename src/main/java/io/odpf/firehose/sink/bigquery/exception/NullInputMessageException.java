package io.odpf.firehose.sink.bigquery.exception;

public class NullInputMessageException extends RuntimeException {
    public NullInputMessageException(long offset) {
        super(String.format("null value in message at %d offset", offset));
    }
}
