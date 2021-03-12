package com.gojek.esb.sink.prometheus;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.config.converter.RangeToHashMapConverter;
import com.gojek.esb.consumer.Message;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.exception.NeedToRetry;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.prometheus.request.PromRequest;
import cortexpb.Cortex;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicHeader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(MockitoJUnitRunner.class)
public class PromSinkTest {
    @Mock
    private Instrumentation instrumentation;
    @Mock
    private PromRequest request;
    @Mock
    private HttpClient httpClient;
    @Mock
    private StencilClient stencilClient;
    @Mock
    private HttpPost httpPost;
    @Mock
    private HttpResponse response;
    @Mock
    private HttpEntity httpEntity;
    @Mock
    private HttpEntity httpEntityResponse;
    @Mock
    private StatusLine statusLine;
    @Mock
    private Map<Integer, Boolean> retryStatusCodeRange;
    @Mock
    private Map<Integer, Boolean> requestLogStatusCodeRanges;

    private List<Message> messages;
    private Cortex.WriteRequest writeRequest;

    @Before
    public void setup() {
        initMocks(this);

        Cortex.Sample sample = Cortex.Sample.newBuilder().setValue(10).setTimestampMs(1000000).build();
        Cortex.LabelPair labelPair = Cortex.LabelPair.newBuilder().setName("__name__").setValue("test_metric").build();
        Cortex.TimeSeries timeSeries = Cortex.TimeSeries.newBuilder()
                .addSamples(sample).addLabels(labelPair)
                .build();
        writeRequest = Cortex.WriteRequest.newBuilder().addTimeseries(timeSeries).build();

        messages = new ArrayList<>();
        Message message = new Message(null, writeRequest.toByteArray(), "", 0, 1);

        messages.add(message);
    }

    @Test
    public void shouldPrepareRequestDuringPreparationAndCallItDuringExecution() throws Exception {
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(request.build(messages)).thenReturn(httpPost);
        when(httpClient.execute(httpPost)).thenReturn(response);
        when(response.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(response.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(Snappy.compress(writeRequest.toByteArray())));

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();

        verify(request, times(1)).build(messages);
        verify(httpClient, times(1)).execute(httpPost);
    }

    @Test(expected = NeedToRetry.class)
    public void shouldThrowNeedToRetryExceptionWhenResponseCodeIsGivenRange() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(messages)).thenReturn(httpPost);
        when(httpClient.execute(httpPost)).thenReturn(response);
        when(response.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(response.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(Snappy.compress(writeRequest.toByteArray())));

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient,
                new RangeToHashMapConverter().convert(null, "400-505"), requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();
    }

    @Test(expected = NeedToRetry.class)
    public void shouldThrowNeedToRetryExceptionWhenResponseCodeIsNull() throws Exception {
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(httpPost.getAllHeaders()).thenReturn(new Header[]{});
        when(httpPost.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(Snappy.compress(writeRequest.toByteArray())));
        when(request.build(messages)).thenReturn(httpPost);
        when(httpClient.execute(httpPost)).thenReturn(null);


        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();
    }

    @Test(expected = IOException.class)
    public void shouldCatchURISyntaxExceptionAndThrowIOException() throws URISyntaxException, DeserializerException, IOException {
        when(request.build(messages)).thenThrow(new URISyntaxException("", ""));

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.prepare(messages);
    }

    @Test
    public void shouldCatchIOExceptionInAbstractSinkAndCaptureFailedExecutionTelemetry() throws Exception {
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(messages)).thenReturn(httpPost);
        when(httpClient.execute(any(HttpPost.class))).thenThrow(IOException.class);

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.pushMessage(messages);

        verify(instrumentation, times(1)).captureFailedExecutionTelemetry(any(IOException.class), anyInt());
    }

    @Test
    public void shouldCloseStencilClient() throws IOException {
        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);

        promSink.close();
        verify(stencilClient, times(1)).close();
    }

    @Test
    public void shouldLogConnectionClosing() throws IOException {
        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);

        promSink.close();
        verify(instrumentation, times(1)).logInfo("HTTP connection closing");
    }

    @Test
    public void shouldLogEntireRequestIfInStatusCodeRangeAndCaptureDroppedMessages() throws Exception {
        String body = "[timeseries {\n  labels {\n    name: \"__name__\"\n    value: \"test_metric\"\n  }\n  samples {\n    value: 10.0\n    timestamp_ms: 1000000\n  }\n}\n]";
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);
        when(httpPost.getMethod()).thenReturn("POST");
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(httpPost.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(httpPost.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(Snappy.compress(writeRequest.toByteArray())));
        when(request.build(messages)).thenReturn(httpPost);
        when(httpClient.execute(httpPost)).thenReturn(response);
        when(response.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(response.getEntity()).thenReturn(httpEntityResponse);
        when(httpEntityResponse.getContent()).thenReturn(new ByteArrayInputStream(new byte[]{}));

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, new RangeToHashMapConverter().convert(null, "400-505"));
        promSink.prepare(messages);
        promSink.execute();
        verify(instrumentation, times(1)).logInfo(
                "\nRequest Method: POST"
                        + "\nRequest Url: http://dummy.com"
                        + "\nRequest Headers: [Accept: text/plain]"
                        + "\nRequest Body: " + body);
        verify(instrumentation, times(1)).logInfo("Message dropped because of status code: 500");
        verify(instrumentation, times(1)).captureCountWithTags("messages.dropped.count", 1, "cause= 500");
    }

    @Test
    public void shouldNotLogEntireRequestIfNotInStatusCodeRange() throws Exception {
        String body = "[timeseries {\n  labels {\n    name: \"__name__\"\n    value: \"test_metric\"\n  }\n  samples {\n    value: 10.0\n    timestamp_ms: 1000000\n  }\n}\n]";

        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(httpPost.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(Snappy.compress(writeRequest.toByteArray())));
        when(request.build(messages)).thenReturn(httpPost);
        when(httpClient.execute(httpPost)).thenReturn(response);
        when(response.getEntity()).thenReturn(httpEntityResponse);
        when(httpEntityResponse.getContent()).thenReturn(new ByteArrayInputStream(new byte[]{}));

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, new RangeToHashMapConverter().convert(null, "400-499"));
        promSink.prepare(messages);
        promSink.execute();
        verify(instrumentation, times(0)).logInfo(
                "\nRequest Method: POST"
                        + "\nRequest Url: http://dummy.com"
                        + "\nRequest Headers: [Accept: text/plain]"
                        + "\nRequest Body: " + body);
    }

    @Test
    public void shouldCaptureDroppedMessagesMetricsIfNotInStatusCodeRange() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(httpPost.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(Snappy.compress(writeRequest.toByteArray())));
        when(request.build(messages)).thenReturn(httpPost);
        when(httpClient.execute(httpPost)).thenReturn(response);
        when(response.getEntity()).thenReturn(httpEntityResponse);
        when(httpEntityResponse.getContent()).thenReturn(new ByteArrayInputStream(new byte[]{}));

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient,
                new RangeToHashMapConverter().convert(null, "400-499"), requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();
        verify(instrumentation, times(1)).logInfo("Message dropped because of status code: 500");
        verify(instrumentation, times(1)).captureCountWithTags("messages.dropped.count", 1, "cause= 500");
    }

    @Test(expected = NeedToRetry.class)
    public void shouldNotCaptureDroppedMessagesMetricsIfInStatusCodeRange() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(messages)).thenReturn(httpPost);
        when(httpClient.execute(httpPost)).thenReturn(response);
        when(response.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(response.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(Snappy.compress(writeRequest.toByteArray())));

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient,
                new RangeToHashMapConverter().convert(null, "400-600"), requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();
        verify(instrumentation, times(0)).logInfo("Message dropped because of status code: 500");
    }

    @Test
    public void shouldNotCaptureDroppedMessagesMetricsIfStatusCodeIs200() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(messages)).thenReturn(httpPost);
        when(httpClient.execute(httpPost)).thenReturn(response);
        when(response.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(response.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(Snappy.compress(writeRequest.toByteArray())));

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();
        verify(instrumentation, times(0)).logInfo("Message dropped because of status code: 200");
        verify(instrumentation, times(0)).captureCountWithTags("messages.dropped.count", 1, "200");
    }

    @Test
    public void shouldNotCaptureDroppedMessagesMetricsIfStatusCodeIs201() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(201);
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(messages)).thenReturn(httpPost);
        when(httpClient.execute(httpPost)).thenReturn(response);
        when(response.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(response.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(Snappy.compress(writeRequest.toByteArray())));

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();
        verify(instrumentation, times(0)).logInfo("Message dropped because of status code: 201");
        verify(instrumentation, times(0)).captureCountWithTags("messages.dropped.count", 1, "201");
    }

    @Test
    public void shouldCaptureResponseStatusCount() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        URI uri = new URI("http://dummy.com");
        when(httpPost.getURI()).thenReturn(uri);
        when(request.build(messages)).thenReturn(httpPost);
        when(httpClient.execute(httpPost)).thenReturn(response);
        when(response.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(response.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(Snappy.compress(writeRequest.toByteArray())));

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient,
                retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();

        verify(instrumentation, times(1)).captureCountWithTags("http.response.code", 1, "status_code=" + statusLine.getStatusCode(), "url=" + uri.getPath());
    }

    @Test
    public void shouldLogResponseAfterExecution() throws Exception {
        String body = "[timeseries {\n  labels {\n    name: \"__name__\"\n    value: \"test_metric\"\n  }\n  samples {\n    value: 10.0\n    timestamp_ms: 1000000\n  }\n}\n]";

        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(response.getStatusLine()).thenReturn(statusLine, statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(request.build(messages)).thenReturn(httpPost);
        when(httpClient.execute(httpPost)).thenReturn(response);
        when(response.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("Accept", "text/plain")});
        when(response.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(Snappy.compress(writeRequest.toByteArray())));

        PromSink promSink = new PromSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange, requestLogStatusCodeRanges);
        promSink.prepare(messages);
        promSink.execute();

        verify(instrumentation, times(1)).logInfo("Response Status: {}", "200");
        verify(instrumentation, times(1)).logDebug(
                "\nResponse Code: 200"
                        + "\nResponse Headers: [Accept: text/plain]"
                        + "\nResponse Body: " + body);
    }
}
