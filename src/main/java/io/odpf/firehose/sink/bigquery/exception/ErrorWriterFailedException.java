package io.odpf.firehose.sink.bigquery.exception;

/**
 * Class models all exceptions that are generated from the handler while processing errors to DLQ.
 */
public class ErrorWriterFailedException extends RuntimeException {
    public ErrorWriterFailedException(Throwable cause) {
        super(cause);
    }
    public ErrorWriterFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
