package com.gojek.esb.exception;

public class EglcConfigurationException extends RuntimeException {

    public EglcConfigurationException(String message) {
        super(message);
    }

    public EglcConfigurationException(String message, Exception e) {
        super(message, e);
    }
}

