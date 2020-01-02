package com.gojek.esb.latestSink.http.request.header;

import java.util.Map;

import com.gojek.esb.consumer.EsbMessage;

/**
 * SupportParamerizedHeader
 */
public interface SupportParamerizedHeader {

  public Map<String, String> build(EsbMessage esbMessage);
}