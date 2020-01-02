package com.gojek.esb.latestSink.http.request.header;

import java.util.Map;

import com.gojek.esb.consumer.EsbMessage;

public interface SupportParamerizedHeader {

  Map<String, String> build(EsbMessage esbMessage);
}
