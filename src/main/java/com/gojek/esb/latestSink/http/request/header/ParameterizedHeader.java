package com.gojek.esb.latestSink.http.request.header;

import java.util.Map;
import java.util.stream.Collectors;

import com.gojek.esb.config.enums.HttpSinkParameterSourceType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.google.common.base.CaseFormat;

/**
 * ParameterizedHeader append header from parsed EsbMessage.
 */
public class ParameterizedHeader implements SupportParamerizedHeader {
  private ProtoToFieldMapper protoToFieldMapper;
  private HttpSinkParameterSourceType httpSinkParameterSource;
  private BasicHeader basicHeader;

  public ParameterizedHeader(ProtoToFieldMapper protoToFieldMapper, HttpSinkParameterSourceType httpSinkParameterSource,
      BasicHeader basicHeader) {
    this.protoToFieldMapper = protoToFieldMapper;
    this.httpSinkParameterSource = httpSinkParameterSource;
    this.basicHeader = basicHeader;
  }

  @Override
  public Map<String, String> build(EsbMessage esbMessage) {
    Map<String, String> headers = basicHeader.build();

    Map<String, Object> paramMap = protoToFieldMapper
        .getFields((httpSinkParameterSource == HttpSinkParameterSourceType.KEY) ? esbMessage.getLogKey()
            : esbMessage.getLogMessage());

    Map<String, String> parameterizedHeaders = paramMap.entrySet().stream()
        .collect(Collectors.toMap(e -> convertToCustomHeaders(e.getKey()), e -> e.getValue().toString()));

    headers.putAll(parameterizedHeaders);
    return headers;
  }

  private String convertToCustomHeaders(String parameter) {
    String customHeader = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, parameter);
    customHeader = "X-" + customHeader;
    return customHeader;
  }

}
