package com.gojek.esb.sink.http;

public class NeedToRetry extends Exception {
  public NeedToRetry(String statusCode) {
    super(String.format("Status code fall under retry range. StatusCode: %s", statusCode));
  }
}
