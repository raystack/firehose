package io.odpf.firehose.exception;

public class FirehoseConsumerFailedException extends RuntimeException {
    public FirehoseConsumerFailedException(Throwable th) {
        super(th);
    }
}
