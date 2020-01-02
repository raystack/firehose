package com.gojek.esb.latestSink.http.request.uri;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;

public class BasicUriTest {

  @Test
  public void shouldReturnURIInstanceBasedOnBaseUrl() {
    BasicUri basicUri = new BasicUri("http://dummy.com");
    try {
      assertEquals(new URI("http://dummy.com"), basicUri.build());
    } catch (URISyntaxException e) {
      new RuntimeException(e);
    }
  }
}
