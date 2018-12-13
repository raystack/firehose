package com.gojek.esb.sink.http.client;

import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.util.Clock;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class HttpSinkClientTest {

    @Mock
    private HttpClient httpClient;

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private HttpEntityEnclosingRequestBase method;

    @Mock
    private HttpResponse httpResponse;

    @Mock
    private StatusLine statusLine;

    @Mock
    private Clock clock;

    @Mock
    private Logger logger;

    @Before
    public void setUp() {
        initMocks(this);
    }

    @Test
    public void shouldReturnNullResponseIfClientThrowsException() throws IOException, URISyntaxException {
        when(method.getURI()).thenReturn(new URI("testPath"));
        when(statusLine.getStatusCode()).thenReturn(200);
        doThrow(new IOException("error")).when(httpClient).execute(method);
        HttpSinkClient httpSinkClient = new HttpSinkClient() {
            @Override
            public HttpClient getHttpClient() {
                return httpClient;
            }

            @Override
            public Clock getClock() {
                return clock;
            }

            @Override
            public StatsDReporter getStatsDReporter() {
                return statsDReporter;
            }

            @Override
            public Logger getLogger() {
                return logger;
            }
        };
        assertEquals(httpSinkClient.sendRequest(method), null);
    }

    @Test
    public void shouldReturnBackResponseIfNoException() throws IOException, URISyntaxException {
        Mockito.when(httpClient.execute(method)).thenReturn(httpResponse);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(method.getURI()).thenReturn(new URI("testPath"));
        HttpSinkClient httpSinkClient = new HttpSinkClient() {
            @Override
            public HttpClient getHttpClient() {
                return httpClient;
            }

            @Override
            public Clock getClock() {
                return clock;
            }

            @Override
            public StatsDReporter getStatsDReporter() {
                return statsDReporter;
            }

            @Override
            public Logger getLogger() {
                return logger;
            }
        };
        assertEquals(httpSinkClient.sendRequest(method), httpResponse);
    }

    @Test
    public void shouldRecordExecutionTime() throws IOException, URISyntaxException {
        Instant beforeCall = Instant.now();
        Instant afterCall = beforeCall.plusSeconds(1);
        Mockito.when(httpClient.execute(method)).thenReturn(httpResponse);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(method.getURI()).thenReturn(new URI("testPath"));
        HttpSinkClient httpSinkClient = new HttpSinkClient() {
            @Override
            public HttpClient getHttpClient() {
                return httpClient;
            }

            @Override
            public Clock getClock() {
                return clock;
            }

            @Override
            public StatsDReporter getStatsDReporter() {
                return statsDReporter;
            }

            @Override
            public Logger getLogger() {
                return logger;
            }
        };
        when(clock.now()).thenReturn(beforeCall).thenReturn(afterCall);

        when(httpClient.execute(method)).thenReturn(httpResponse);
        httpSinkClient.sendRequest(method);
        verify(statsDReporter).captureDurationSince("http.execution_time", beforeCall);
    }
}
