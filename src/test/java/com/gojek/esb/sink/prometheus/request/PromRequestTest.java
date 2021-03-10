package com.gojek.esb.sink.prometheus.request;

import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.consumer.Message;
import com.gojek.esb.consumer.TestFeedbackLogMessage;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.http.request.header.HeaderBuilder;
import com.gojek.esb.sink.http.request.uri.UriBuilder;
import com.gojek.esb.sink.prometheus.builder.RequestEntityBuilder;
import com.gojek.esb.sink.prometheus.builder.TimeSeriesBuilder;
import com.gojek.esb.sink.prometheus.builder.WriteRequestBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import cortexpb.Cortex;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.MockitoAnnotations.initMocks;


public class PromRequestTest {

    @Mock
    private UriBuilder uriBuilder;

    @Mock
    private HeaderBuilder headerBuilder;

    @Mock
    private ProtoParser protoParser;

    @Mock
    private RequestEntityBuilder requestEntityBuilder;

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private TimeSeriesBuilder timeSeriesBuilder;

    @Mock
    private WriteRequestBuilder writeRequestBuilder;

    @Before
    public void setUp() {
        initMocks(this);
    }

    @Test
    public void shouldProcessMessagesInBatch() throws URISyntaxException, IOException {
        Message message = new Message("key".getBytes(), "msg".getBytes(), "topic", 0, 100);
        List<Message> messages = Arrays.asList(message, message, message);

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setTipAmount(100)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(10000)).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());
        Cortex.LabelPair labelPair1 = Cortex.LabelPair.newBuilder().setName(ByteString.copyFromUtf8("__name__")).setValue(ByteString.copyFromUtf8("tip_amount")).build();
        Cortex.LabelPair labelPair2 = Cortex.LabelPair.newBuilder().setName(ByteString.copyFromUtf8("kafka_partition")).setValue(ByteString.copyFromUtf8("0")).build();
        Cortex.Sample sample = Cortex.Sample.newBuilder().setTimestampMs(10000).setValue(100).build();

        Cortex.TimeSeries timeSeries = Cortex.TimeSeries.newBuilder()
                .addLabels(labelPair1).addSamples(sample)
                .addLabels(labelPair2)
                .build();

        Cortex.WriteRequest writeRequestBody = Cortex.WriteRequest.newBuilder()
                .addTimeseries(timeSeries)
                .build();

        when(timeSeriesBuilder.buildTimeSeries(dynamicMessage, message.getPartition())).thenReturn(Collections.singletonList(timeSeries));
        when(protoParser.parse(message.getLogMessage())).thenReturn(dynamicMessage);
        when(writeRequestBuilder.buildWriteRequest(messages)).thenReturn(writeRequestBody);

        PromRequest promRequest = new PromRequest(statsDReporter, writeRequestBuilder)
                .setRequest(headerBuilder, uriBuilder, requestEntityBuilder);
        promRequest.build(messages);

        verify(uriBuilder, times(1)).build();
        verify(headerBuilder, times(1)).build();
        verify(requestEntityBuilder, times(1)).buildHttpEntity(writeRequestBody);
    }
}
