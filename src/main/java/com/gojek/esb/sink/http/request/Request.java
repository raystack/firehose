package com.gojek.esb.sink.http.request;

import java.net.URISyntaxException;
import java.util.List;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;

import org.apache.http.client.methods.HttpPut;

/**
 * Request interface for building HTTP method. Request will be used by {@link HttpSink} to make actual call.
 */
public interface Request {

  List<HttpPut> build(List<EsbMessage> esbMessages) throws URISyntaxException, DeserializerException;
}
