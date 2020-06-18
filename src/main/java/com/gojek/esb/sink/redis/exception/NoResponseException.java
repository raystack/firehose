package com.gojek.esb.sink.redis.exception;

/**
 * NoResponseException
 * <p>
 * Exception to raise if there is no responds from redisClient.
 */
public class NoResponseException extends RuntimeException {

    public NoResponseException() {
        super("Redis Pipeline error: no responds received");
    }
}
