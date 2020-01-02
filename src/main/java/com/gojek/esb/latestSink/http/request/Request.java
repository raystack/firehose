package com.gojek.esb.latestSink.http.request;

import java.net.URISyntaxException;
import java.util.List;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;

import org.apache.http.client.methods.HttpPut;

/**
 * Request
 */
public interface Request {

  public List<HttpPut> build(List<EsbMessage> esbMessages) throws URISyntaxException, DeserializerException;
}