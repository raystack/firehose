package io.odpf.firehose.exception;

public class SinkException extends RuntimeException {
    public SinkException(String message, Throwable cause) {
        super(message, cause);
    }
}
