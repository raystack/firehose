package com.gojek.esb.config;

public class EglcConfigurationException extends RuntimeException {

    public EglcConfigurationException(String message){
        super(message);
    }

    public EglcConfigurationException(String message, Exception e){
        super(message, e);
    }
}
