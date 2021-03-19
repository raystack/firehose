package io.odpf.firehose.exception;

public class DeserializerException extends RuntimeException {

    public DeserializerException(String message) {
        super(message);
    }

    public DeserializerException(String message, Exception e) {
        super(message, e);
    }
}
