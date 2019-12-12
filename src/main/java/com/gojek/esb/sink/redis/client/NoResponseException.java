package com.gojek.esb.sink.redis.client;
/**
 * NoResponseException
 */
public class NoResponseException extends RuntimeException {

  NoResponseException() {
    super("Redis Pipeline error: no responds received");
  }
}
