package com.gojek.esb.latestSink.http.request.uri;

import java.net.URI;
import java.net.URISyntaxException;

import com.gojek.esb.consumer.EsbMessage;

/**
 * SupportParameterizedUri interface for request that need parameterized URI.
 */
public interface SupportParameterizedUri {

  URI build(EsbMessage esbMessage) throws URISyntaxException;
}
