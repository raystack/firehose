package io.odpf.firehose.sink.prometheus.request;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.http.request.uri.UriBuilder;
import io.odpf.firehose.sink.prometheus.builder.HeaderBuilder;
import io.odpf.firehose.sink.prometheus.builder.RequestEntityBuilder;
import io.odpf.firehose.sink.prometheus.builder.WriteRequestBuilder;
import cortexpb.Cortex;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;


public class PromRequestTest {

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private HeaderBuilder headerBuilder;

    @Mock
    private UriBuilder uriBuilder;

    @Mock
    private RequestEntityBuilder requestEntityBuilder;

    @Mock
    private WriteRequestBuilder writeRequestBuilder;

    @Before
    public void setUp() {
        initMocks(this);
    }

    @Test
    public void shouldProperlyBuildMessages() throws URISyntaxException, IOException {
        Message message = new Message("key".getBytes(), "msg".getBytes(), "topic", 0, 100);
        List<Message> messages = Arrays.asList(message, message, message);

        Cortex.LabelPair labelPair1 = Cortex.LabelPair.newBuilder().setName("__name__").setValue("tip_amount").build();
        Cortex.LabelPair labelPair2 = Cortex.LabelPair.newBuilder().setName("kafka_partition").setValue("0").build();
        Cortex.Sample sample = Cortex.Sample.newBuilder().setTimestampMs(10000).setValue(100).build();

        Cortex.TimeSeries timeSeries = Cortex.TimeSeries.newBuilder()
                .addLabels(labelPair1).addSamples(sample)
                .addLabels(labelPair2)
                .build();

        Cortex.WriteRequest writeRequestBody = Cortex.WriteRequest.newBuilder()
                .addTimeseries(timeSeries)
                .build();

        HashMap<String, String> headerMap = new HashMap<>();
        headerMap.put("Content-Encoding", "snappy");
        headerMap.put("X-Prometheus-Remote-Write-Version", "0.1.0");

        URI uri = new URI("dummyEndpoint");
        when(writeRequestBuilder.buildWriteRequest(messages)).thenReturn(writeRequestBody);
        when(uriBuilder.build()).thenReturn(uri);
        when(headerBuilder.build()).thenReturn(headerMap);

        PromRequest promRequest = new PromRequest(instrumentation, headerBuilder, uriBuilder, requestEntityBuilder, writeRequestBuilder);
        promRequest.build(messages);

        verify(uriBuilder, times(1)).build();
        verify(headerBuilder, times(1)).build();
        verify(writeRequestBuilder, times(1)).buildWriteRequest(messages);
        verify(requestEntityBuilder, times(1)).buildHttpEntity(writeRequestBody);
        verify(instrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}", uri, headerMap, writeRequestBody.toString());
    }
}
