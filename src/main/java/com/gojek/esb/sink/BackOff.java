package com.gojek.esb.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BackOff {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(ExponentialBackOffProvider.class);

    public void inMilliSeconds(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            LOGGER.error("Backoff thread sleep for {} milliseconds interrupted : {} {}",
                    milliseconds, e.getClass(), e.getMessage());
        }
    }
}
