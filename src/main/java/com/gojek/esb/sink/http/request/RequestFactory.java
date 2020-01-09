package com.gojek.esb.sink.http.request;

import static com.gojek.esb.config.enums.HttpSinkParameterPlacementType.HEADER;

import java.util.Map;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.HTTPSinkConfig;
import com.gojek.esb.config.ParameterizedHTTPSinkConfig;
import com.gojek.esb.config.enums.HttpSinkParameterPlacementType;
import com.gojek.esb.config.enums.HttpSinkParameterSourceType;
import com.gojek.esb.sink.http.factory.SerializerFactory;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.header.BasicHeader;
import com.gojek.esb.sink.http.request.header.ParameterizedHeader;
import com.gojek.esb.sink.http.request.uri.BasicUri;
import com.gojek.esb.sink.http.request.uri.ParameterizedUri;

import com.gojek.esb.proto.ProtoToFieldMapper;
import com.gojek.esb.serializer.EsbMessageSerializer;

import org.aeonbits.owner.ConfigFactory;

public class RequestFactory {

  private Map<String, String> configuration;
  private HTTPSinkConfig httpSinkConfig;
  private StencilClient stencilClient;

  public RequestFactory(Map<String, String> configuration, StencilClient stencilClient) {
    this.configuration = configuration;
    this.stencilClient = stencilClient;
    httpSinkConfig = ConfigFactory.create(HTTPSinkConfig.class, configuration);
  }

  public Request create() {
    if (httpSinkConfig.getHttpSinkParameterSource() == HttpSinkParameterSourceType.DISABLED) {
      BasicUri basicUri = new BasicUri(httpSinkConfig.getServiceURL());
      BasicHeader basicHeader = new BasicHeader(httpSinkConfig.getHTTPHeaders());
      return new BatchRequest(basicUri, basicHeader, createBody());
    }


    ParameterizedHTTPSinkConfig parameterizedHttpSinkConfig = ConfigFactory.create(ParameterizedHTTPSinkConfig.class, configuration);

    ProtoParser protoParser = new ProtoParser(stencilClient, parameterizedHttpSinkConfig.getParameterProtoSchema());
    ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(protoParser, parameterizedHttpSinkConfig.getProtoToFieldMapping());

    HttpSinkParameterPlacementType placementType =  parameterizedHttpSinkConfig.getHttpSinkParameterPlacement();
    HttpSinkParameterSourceType parameterSource = parameterizedHttpSinkConfig.getHttpSinkParameterSource();
    String headers = parameterizedHttpSinkConfig.getHTTPHeaders();
    if (placementType == HEADER) {
      BasicUri basicUri = new BasicUri(httpSinkConfig.getServiceURL());
      ParameterizedHeader parameterizedHeader = new ParameterizedHeader(protoToFieldMapper, parameterSource, new BasicHeader(headers));
      return new MultipleRequest(basicUri, parameterizedHeader, createBody());

    } else {
      ParameterizedUri parameterizedUri = new ParameterizedUri(httpSinkConfig.getServiceURL(), protoToFieldMapper, parameterSource);
      BasicHeader basicHeader = new BasicHeader(headers);
      return new MultipleRequest(parameterizedUri, basicHeader, createBody());
    }
  }

  private JsonBody createBody() {
    EsbMessageSerializer esbMessageSerializer = new SerializerFactory(
      httpSinkConfig.getHttpSinkDataFormat(),
      httpSinkConfig.getProtoSchema(),
      stencilClient).build();
    return new JsonBody(esbMessageSerializer);
  }

}
