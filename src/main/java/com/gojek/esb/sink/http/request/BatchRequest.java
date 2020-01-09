package com.gojek.esb.sink.http.request;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.header.BasicHeader;
import com.gojek.esb.sink.http.request.uri.BasicUri;

import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchRequest implements Request {
  private BasicUri uri;
  private BasicHeader header;
  private JsonBody body;

  private static final Logger LOGGER = LoggerFactory.getLogger(BatchRequest.class);

  public BatchRequest(BasicUri uri, BasicHeader header, JsonBody body) {
    this.uri = uri;
    this.header = header;
    this.body = body;
  }

  public List<HttpPut> build(List<EsbMessage> esbMessages) throws DeserializerException, URISyntaxException {
    HttpPut httpPut = new HttpPut(uri.build());
    header.build().forEach(httpPut::addHeader);
    httpPut.setEntity(buildHttpEntity(esbMessages));

    LOGGER.debug("Request URL: {}", uri.build());
    LOGGER.debug("Request headers: {}", header.build());
    LOGGER.debug("Request content: {}", body.serialize(esbMessages));

    return Collections.singletonList(httpPut);
  }

  private StringEntity buildHttpEntity(List<EsbMessage> esbMessages) throws DeserializerException {
    return new StringEntity(body.serialize(esbMessages).toString(), ContentType.APPLICATION_JSON);
  }

}
