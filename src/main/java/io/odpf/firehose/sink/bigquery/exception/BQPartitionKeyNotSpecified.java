package io.odpf.firehose.sink.bigquery.exception;

public class BQPartitionKeyNotSpecified extends RuntimeException {
    public BQPartitionKeyNotSpecified(String message) {
        super(message);
    }
}
