package io.odpf.firehose.sink.bigquery.exception;

public class BQDatasetLocationChangedException extends RuntimeException {
    public BQDatasetLocationChangedException(String message) {
        super(message);
    }
}

