package com.gojek.esb.filter;

public class EsbFilterException extends Exception {

    public EsbFilterException(String message, Exception e) {
        super(message, e);
    }

    public EsbFilterException(String message) {
        super(message);
    }
}
