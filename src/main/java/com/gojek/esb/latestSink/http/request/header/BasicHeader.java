package com.gojek.esb.latestSink.http.request.header;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.gojek.esb.consumer.EsbMessage;

/**
 * BaseHeaderGenerator create header from headerConfig.
 */
public class BasicHeader implements SupportParamerizedHeader {

  String headerConfig;

  public BasicHeader(String headerConfig) {
    this.headerConfig = headerConfig;
  }

  public Map<String, String> build() {
    return (HashMap<String, String>) Arrays.stream(headerConfig.split(","))
        .filter(headerKeyValue -> !headerKeyValue.trim().isEmpty()).collect(Collectors
            .toMap(headerKeyValue -> headerKeyValue.split(":")[0], headerKeyValue -> headerKeyValue.split(":")[1]));
  }

  @Override
  public Map<String, String> build(EsbMessage esbMessage) {
    return build();
  }
}
