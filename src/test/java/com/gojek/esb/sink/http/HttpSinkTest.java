package com.gojek.esb.sink.http;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.config.converter.RangeToHashMapConverter;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.http.request.Request;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.gradle.internal.impldep.org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HttpSinkTest {
  @Mock
  Instrumentation instrumentation;
  @Mock
  Request request;
  @Mock
  HttpClient httpClient;
  @Mock
  StencilClient stencilClient;
  @Mock
  HttpPut httpPut;
  @Mock
  HttpResponse response;
  @Mock
  StatusLine statusLine;
  @Mock
  Map<Integer, Boolean> retryStatusCodeRange;

  private List<EsbMessage> esbMessages;

  @Before
  public void setup() {
    EsbMessage esbMessage = new EsbMessage(new byte[] { 10, 20 }, new byte[] { 1, 2 }, "sample-topic", 0, 100);
    esbMessages = Collections.singletonList(esbMessage);
  }

  @Test
  public void shouldPrepareRequestDuringPreparationAndCallItDuringExecution() throws Exception{
    when(response.getStatusLine()).thenReturn(statusLine, statusLine);
    when(statusLine.getStatusCode()).thenReturn(200, 200);

    List<HttpPut> httpPuts = Arrays.asList(httpPut, httpPut);
    when(request.build(esbMessages)).thenReturn(httpPuts);
    when(httpClient.execute(httpPut)).thenReturn(response, response);

    HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange);
    httpSink.prepare(esbMessages);
    httpSink.execute();

    verify(request, times(1)).build(esbMessages);
    verify(httpClient, times(2)).execute(httpPut);

  }

  @Test(expected = NeedToRetry.class)
  public void shouldThrowErrorWhenResponseCodeIsGivenRange() throws Exception {
    when(response.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(500);

    List<HttpPut> httpPuts = Arrays.asList(httpPut);
    when(request.build(esbMessages)).thenReturn(httpPuts);
    when(httpClient.execute(httpPut)).thenReturn(response);

    Map<Integer, Boolean> retryStatusCodeRange = new RangeToHashMapConverter().convert(null, "400-505");

    HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange);
    httpSink.prepare(esbMessages);
    httpSink.execute();
  }

  @Test(expected = NeedToRetry.class)
  public void shouldThrowErrorWhenResponseCodeIsNull() throws Exception {

    List<HttpPut> httpPuts = Arrays.asList(httpPut);
    when(request.build(esbMessages)).thenReturn(httpPuts);
    when(httpClient.execute(httpPut)).thenReturn(null);

    Map<Integer, Boolean> retryStatusCodeRange = new RangeToHashMapConverter().convert(null, "400-505");

    HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange);
    httpSink.prepare(esbMessages);
    httpSink.execute();
  }

}
