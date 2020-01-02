package com.gojek.esb.latestSink.http.request.uri;

import java.net.URI;
import java.net.URISyntaxException;

import com.gojek.esb.consumer.EsbMessage;

/**
 * SupportParameterizedUri
 */
public interface SupportParameterizedUri {

  public URI build(EsbMessage esbMessage) throws URISyntaxException;
}