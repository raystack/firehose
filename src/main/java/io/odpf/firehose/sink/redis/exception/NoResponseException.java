package io.odpf.firehose.sink.redis.exception;

/**
 * NoResponseException
 * <p>
 * Exception to raise if there is no responds from redisClient.
 */
public class NoResponseException extends RuntimeException {

    /**
     * Instantiates a new No response exception.
     */
    public NoResponseException() {
        super("Redis Pipeline error: no responds received");
    }
}
