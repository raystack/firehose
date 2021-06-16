package io.odpf.firehose.exception;

public class BQPartitionKeyNotSpecified extends RuntimeException {
    public BQPartitionKeyNotSpecified(String message){
        super(message);
    }
}
