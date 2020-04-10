package com.gojek.esb.sink.http.request.uri;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import com.gojek.esb.config.enums.HttpSinkParameterSourceType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.proto.ProtoToFieldMapper;

import org.apache.http.client.utils.URIBuilder;

/**
 * ParameterizedUri encapsulate URI creation based on EsbMessage as params.
 */
public class ParameterizedUri implements SupportParameterizedUri {

  private String baseUrl;
  private ProtoToFieldMapper protoToFieldMapper;
  private HttpSinkParameterSourceType httpSinkParameterSource;

  public ParameterizedUri(String baseUrl, ProtoToFieldMapper protoToFieldMapper,
      HttpSinkParameterSourceType httpSinkParameterSource) {
    this.baseUrl = baseUrl;
    this.protoToFieldMapper = protoToFieldMapper;
    this.httpSinkParameterSource = httpSinkParameterSource;
  }

  @Override
  public URI build(EsbMessage esbMessage, UriParser uriParser) throws URISyntaxException {
    Map<String, Object> paramMap = protoToFieldMapper
        .getFields((httpSinkParameterSource == HttpSinkParameterSourceType.KEY) ? esbMessage.getLogKey()
            : esbMessage.getLogMessage());
    String parsedUrl = uriParser.parse(esbMessage, baseUrl);
    URIBuilder uriBuilder = new URIBuilder(parsedUrl);
    paramMap.forEach((string, object) -> uriBuilder.addParameter(string, object.toString()));

    return uriBuilder.build();
  }

}
