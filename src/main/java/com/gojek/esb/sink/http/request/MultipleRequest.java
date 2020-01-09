package com.gojek.esb.sink.http.request;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.header.SupportParamerizedHeader;
import com.gojek.esb.sink.http.request.uri.SupportParameterizedUri;

import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MultipleRequest create one HttpPut per-message. Uri and Header are
 * parametrized according to incoming message.
 */
public class MultipleRequest implements Request {

  private SupportParameterizedUri parameterizedUri;
  private SupportParamerizedHeader parameterizedHeader;
  private JsonBody body;

  private static final Logger LOGGER = LoggerFactory.getLogger(MultipleRequest.class);

  public MultipleRequest(SupportParameterizedUri parameterizedUri, SupportParamerizedHeader parameterizedHeader, JsonBody body) {
    this.parameterizedUri = parameterizedUri;
    this.parameterizedHeader = parameterizedHeader;
    this.body = body;
  }

  public List<HttpPut> build(List<EsbMessage> esbMessages) throws URISyntaxException, DeserializerException {
    List<HttpPut> httpPuts = new ArrayList<HttpPut>();
    List<String> bodyContents = body.serialize(esbMessages);
    for (int i = 0; i < esbMessages.size(); i++) {
      EsbMessage esbMessage = esbMessages.get(i);
      HttpPut request = new HttpPut(parameterizedUri.build(esbMessage));
      parameterizedHeader.build(esbMessage).forEach(request::addHeader);
      request.setEntity(buildHttpEntity(bodyContents.get(i)));
      httpPuts.add(request);

      LOGGER.debug("Request URL: {}", parameterizedUri.build(esbMessage));
      LOGGER.debug("Request headers: {}", parameterizedHeader.build(esbMessage));
      LOGGER.debug("Request content: {}", bodyContents.get(i));
    }

    return httpPuts;
  }

  private StringEntity buildHttpEntity(String bodyContent) {
    return new StringEntity(bodyContent, ContentType.APPLICATION_JSON);
  }
}
