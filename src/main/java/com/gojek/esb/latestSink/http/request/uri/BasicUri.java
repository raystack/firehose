package com.gojek.esb.latestSink.http.request.uri;

import java.net.URI;
import java.net.URISyntaxException;

import com.gojek.esb.consumer.EsbMessage;

/**
 * BasicUri encapsulate simple URI instance creation.
 */
public class BasicUri implements SupportParameterizedUri {
  private String baseUrl;

  public BasicUri(String uri) {
    this.baseUrl = uri;
  }

  public URI build() throws URISyntaxException {
    return new URI(baseUrl);
  }

  @Override
  public URI build(EsbMessage esbMessage) throws URISyntaxException {
    return build();
  }

}
