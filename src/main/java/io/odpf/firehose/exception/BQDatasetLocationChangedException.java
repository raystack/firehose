package io.odpf.firehose.exception;

public class BQDatasetLocationChangedException extends RuntimeException {
    public BQDatasetLocationChangedException(String s) {
        super(s);
    }
}
