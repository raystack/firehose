package com.gojek.esb.sink.http.request;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.gojek.esb.config.enums.HttpRequestMethod;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.header.SupportParamerizedHeader;
import com.gojek.esb.sink.http.request.uri.SupportParameterizedUri;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ParameterizedRequest create one HttpPut per-message. Uri and Header are
 * parametrized according to incoming message.
 */
public class ParameterizedRequest implements Request {

  private SupportParameterizedUri parameterizedUri;
  private SupportParamerizedHeader parameterizedHeader;
  private JsonBody body;
  private HttpRequestMethod method;

  private static final Logger LOGGER = LoggerFactory.getLogger(ParameterizedRequest.class);

  public ParameterizedRequest(SupportParameterizedUri parameterizedUri, SupportParamerizedHeader parameterizedHeader, JsonBody body, HttpRequestMethod method) {
    this.parameterizedUri = parameterizedUri;
    this.parameterizedHeader = parameterizedHeader;
    this.body = body;
    this.method = method;
  }

  public List<HttpEntityEnclosingRequestBase> build(List<EsbMessage> esbMessages) throws URISyntaxException, DeserializerException {
    List<HttpEntityEnclosingRequestBase> requests = new ArrayList<>();
    List<String> bodyContents = body.serialize(esbMessages);
    for (int i = 0; i < esbMessages.size(); i++) {
      EsbMessage esbMessage = esbMessages.get(i);
      HttpEntityEnclosingRequestBase request = HttpRequestMethodFactory.create(parameterizedUri.build(esbMessage), method);
      parameterizedHeader.build(esbMessage).forEach(request::addHeader);
      request.setEntity(buildHttpEntity(bodyContents.get(i)));
      requests.add(request);

      LOGGER.debug("Request URL: {}", parameterizedUri.build(esbMessage));
      LOGGER.debug("Request headers: {}", parameterizedHeader.build(esbMessage));
      LOGGER.debug("Request content: {}", bodyContents.get(i));
      LOGGER.debug("Request method: {}", method);

    }

    return requests;
  }

  private StringEntity buildHttpEntity(String bodyContent) {
    String arrayWrappedBody = Collections.singletonList(bodyContent).toString();
    return new StringEntity(arrayWrappedBody, ContentType.APPLICATION_JSON);
  }
}
