package com.gojek.esb.sink.redis.client;
/**
 * NoResponseException
 * <p>
 * Exception to raise if there is no responds from redisClient.
 */
public class NoResponseException extends RuntimeException {

  NoResponseException() {
    super("Redis Pipeline error: no responds received");
  }
}
