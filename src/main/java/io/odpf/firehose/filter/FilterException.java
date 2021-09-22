package io.odpf.firehose.filter;

public class FilterException extends RuntimeException {

    public FilterException(String message, Exception e) {
        super(message, e);
    }

    public FilterException(String message) {
        super(message);
    }
}
