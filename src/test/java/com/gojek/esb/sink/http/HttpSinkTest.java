package com.gojek.esb.sink.http;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.config.converter.RangeToHashMapConverter;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(MockitoJUnitRunner.class)
public class HttpSinkTest {
    @Mock
    private Instrumentation instrumentation;
    @Mock
    private Request request;
    @Mock
    private HttpClient httpClient;
    @Mock
    private StencilClient stencilClient;
    @Mock
    private HttpPut httpPut;
    @Mock
    private HttpResponse response;
    @Mock
    private StatusLine statusLine;
    @Mock
    private Map<Integer, Boolean> retryStatusCodeRange;

    private List<EsbMessage> esbMessages;

    @Before
    public void setup() throws URISyntaxException {
        initMocks(this);
        EsbMessage esbMessage = new EsbMessage(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        esbMessages = Collections.singletonList(esbMessage);
    }

    @Test
    public void shouldPrepareRequestDuringPreparationAndCallItDuringExecution() throws Exception {
        when(httpPut.getURI()).thenReturn(new URI("http://dummy.com"));
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
    public void shouldThrowNeedToRetryExceptionWhenResponseCodeIsGivenRange() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);

        List<HttpPut> httpPuts = Arrays.asList(httpPut);

        when(httpPut.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(esbMessages)).thenReturn(httpPuts);
        when(httpClient.execute(httpPut)).thenReturn(response);

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient,
                new RangeToHashMapConverter().convert(null, "400-505"));
        httpSink.prepare(esbMessages);
        httpSink.execute();
    }

    @Test(expected = NeedToRetry.class)
    public void shouldThrowNeedToRetryExceptionWhenResponseCodeIsNull() throws Exception {

        List<HttpPut> httpPuts = Arrays.asList(httpPut);

        when(httpPut.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(esbMessages)).thenReturn(httpPuts);
        when(httpClient.execute(httpPut)).thenReturn(null);

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange);
        httpSink.prepare(esbMessages);
        httpSink.execute();
    }

    @Test(expected = IOException.class)
    public void shouldCatchURISyntaxExceptionAndThrowIOException() throws URISyntaxException, DeserializerException, IOException {
        when(request.build(esbMessages)).thenThrow(new URISyntaxException("", ""));

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange);
        httpSink.prepare(esbMessages);
    }

    @Test(expected = IOException.class)
    public void shouldCatchIOExceptionAndThrowItCapturingFatalError() throws Exception {
        when(httpPut.getURI()).thenReturn(new URI("http://dummy.com"));

        List<HttpPut> httpPuts = Arrays.asList(httpPut, httpPut);
        when(request.build(esbMessages)).thenReturn(httpPuts);

        IOException e = new IOException();
        when(httpClient.execute(httpPut)).thenThrow(e);

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange);
        httpSink.prepare(esbMessages);
        httpSink.execute();

        verify(instrumentation, times(1)).captureFatalError(e, "Error while calling http sink service url");
    }

    @Test
    public void shouldCloseStencilClient() throws IOException {
        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange);

        httpSink.close();
        verify(stencilClient, times(1)).close();
    }

}
