package io.odpf.firehose.exception;

/**
 * This exception is thrown when there is invalid configuration encountered.
 */
public class EglcConfigurationException extends RuntimeException {

    public EglcConfigurationException(String message) {
        super(message);
    }

    public EglcConfigurationException(String message, Exception e) {
        super(message, e);
    }
}

