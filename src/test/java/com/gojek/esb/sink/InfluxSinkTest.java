package com.gojek.esb.sink;

import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.config.InfluxSinkConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.feedback.FeedbackLogKey;
import com.gojek.esb.feedback.FeedbackLogMessage;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.influxdb.InfluxSink;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import org.aeonbits.owner.ConfigFactory;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class InfluxSinkTest {
    private static final int TIMESTAMP_IN_EPOCH_SECONDS = 1498600;
    private final String measurementName = "FeedbackMeasure";
    private final String databaseName = "test";
    private final String emptyFieldNameIndex = "{}";
    private final String emptyTagNameIndexMapping = "{}";
    private final String orderNumber = "lol";
    private final String feedbackComment = "good";
    private final String driverId = "3";
    private final float tipAmount = 3.14f;
    private final int feedbackOrderNumberIndex = 1;
    private final int feedbackCommentIndex = 6;
    private final int tipAmountIndex = 7;
    private final int driverIdIndex = 3;
    private Sink sink;
    private EsbMessage esbMessage;
    private Point expectedPoint;
    private InfluxSinkConfig config;
    private Properties props = new Properties();
    private Point.Builder pointBuilder;
    private StencilClient stencilClient;
    private List<EsbMessage> esbMessages;

    @Mock
    private InfluxDB client;

    @Mock
    private ProtoParser protoParser;


    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private StencilClient mockStencilClient;

    @Before
    public void setUp() {
        props.setProperty("MEASUREMENT_NAME", measurementName);
        props.setProperty("PROTO_EVENT_TIMESTAMP_INDEX", "2");
        props.setProperty("DATABASE_NAME", databaseName);
        props.setProperty("PROTO_SCHEMA", FeedbackLogMessage.class.getName());

        FeedbackLogMessage feedbackLogMessage = FeedbackLogMessage.newBuilder().setDriverId(driverId).setOrderNumber(orderNumber)
                .setFeedbackComment(feedbackComment).setTipAmount(tipAmount).setEventTimestamp(
                        Timestamp.newBuilder().setSeconds(TIMESTAMP_IN_EPOCH_SECONDS).build()).build();
        FeedbackLogKey feedbackLogKey = FeedbackLogKey.newBuilder().setOrderNumber(orderNumber).build();

        esbMessage = new EsbMessage(feedbackLogKey.toByteArray(), feedbackLogMessage.toByteArray(), databaseName, 1, 1);
        esbMessages = Collections.singletonList(esbMessage);

        pointBuilder = Point.measurement(measurementName).addField("feedbackOrderNumber", orderNumber)
                .addField("comment", feedbackComment).addField("tipAmount", tipAmount).time(TIMESTAMP_IN_EPOCH_SECONDS, TimeUnit.SECONDS);
        stencilClient = StencilClientFactory.getClient();
    }

    @Test
    public void shouldPrepareBatchPoints() throws IOException, DeserializerException {
        setupFieldNameIndexMappingProperties();
        setupTagNameIndexMappingProperties();
        props.setProperty("PROTO_EVENT_TIMESTAMP_INDEX", "5");
        config = ConfigFactory.create(InfluxSinkConfig.class, props);

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(BookingLogMessage.getDescriptor()).build();

        when(protoParser.parse(any())).thenReturn(dynamicMessage);
        InfluxSinkStub influx = new InfluxSinkStub(instrumentation, "influx", config, protoParser, client, stencilClient);

        influx.prepare(esbMessages);
        verify(protoParser, times(1)).parse(esbMessage.getLogMessage());
    }

    @Test
    public void shouldPushTagsAsStringValues() throws DeserializerException, IOException {
        expectedPoint = pointBuilder.tag("driver_id", driverId).build();
        setupFieldNameIndexMappingProperties();
        setupTagNameIndexMappingProperties();
        config = ConfigFactory.create(InfluxSinkConfig.class, props);

        sink = new InfluxSink(instrumentation, "influx", config, new ProtoParser(stencilClient, config.getProtoSchema()), client, stencilClient);

        ArgumentCaptor<BatchPoints> batchPointsArgumentCaptor = ArgumentCaptor.forClass(BatchPoints.class);

        sink.pushMessage(esbMessages);
        verify(client, times(1)).write(batchPointsArgumentCaptor.capture());
        List<BatchPoints> batchPointsList = batchPointsArgumentCaptor.getAllValues();

        assertEquals(expectedPoint.lineProtocol(), batchPointsList.get(0).getPoints().get(0).lineProtocol());
    }

    @Test
    public void shouldThrowExceptionOnEmptyFieldNameIndexMapping() throws IOException, DeserializerException {
        props.setProperty("FIELD_NAME_PROTO_INDEX_MAPPING", emptyFieldNameIndex);
        props.setProperty("TAG_NAME_PROTO_INDEX_MAPPING", emptyTagNameIndexMapping);
        config = ConfigFactory.create(InfluxSinkConfig.class, props);
        sink = new InfluxSink(instrumentation, "influx", config, new ProtoParser(stencilClient, config.getProtoSchema()), client, stencilClient);

        try {
            sink.pushMessage(esbMessages);
        } catch (Exception e) {
            assertEquals(InfluxSink.FIELD_NAME_MAPPING_ERROR_MESSAGE, e.getMessage());
        }
    }

    @Test
    public void shouldCaptureFailedExecutionTelemetryIncaseOfExceptions() throws DeserializerException, IOException {
        expectedPoint = pointBuilder.tag("driver_id", driverId).build();
        setupFieldNameIndexMappingProperties();
        setupTagNameIndexMappingProperties();
        config = ConfigFactory.create(InfluxSinkConfig.class, props);
        sink = new InfluxSink(instrumentation, "influx", config, new ProtoParser(stencilClient, config.getProtoSchema()), client, stencilClient);

        RuntimeException runtimeException = new RuntimeException();
        doThrow(runtimeException).when(instrumentation).startExecution();

        sink.pushMessage(esbMessages);

        verify(instrumentation, times(1)).captureFailedExecutionTelemetry(runtimeException, esbMessages.size());
    }


    @Test
    public void shouldPushMessagesWithType() throws DeserializerException, IOException {
        expectedPoint = pointBuilder.build();
        setupFieldNameIndexMappingProperties();
        props.setProperty("TAG_NAME_PROTO_INDEX_MAPPING", emptyTagNameIndexMapping);
        config = ConfigFactory.create(InfluxSinkConfig.class, props);
        sink = new InfluxSink(instrumentation, "influx", config, new ProtoParser(stencilClient, config.getProtoSchema()), client, stencilClient);
        ArgumentCaptor<BatchPoints> batchPointsArgumentCaptor = ArgumentCaptor.forClass(BatchPoints.class);

        sink.pushMessage(esbMessages);
        verify(instrumentation, times(1)).capturePreExecutionLatencies(esbMessages);
        verify(instrumentation, times(1)).startExecution();
        verify(instrumentation, times(1)).logInfo("pushing {} messages", esbMessages.size());
        verify(client, times(1)).write(batchPointsArgumentCaptor.capture());
        List<BatchPoints> batchPointsList = batchPointsArgumentCaptor.getAllValues();

        assertEquals(expectedPoint.lineProtocol(), batchPointsList.get(0).getPoints().get(0).lineProtocol());
    }

    @Test
    public void shouldCloseStencilClient() throws IOException {
        config = ConfigFactory.create(InfluxSinkConfig.class, props);

        sink = new InfluxSink(instrumentation, "influx", config, new ProtoParser(mockStencilClient, config.getProtoSchema()), client, mockStencilClient);
        sink.close();

        verify(mockStencilClient, times(1)).close();
    }

    private void setupFieldNameIndexMappingProperties() {
        props.setProperty("FIELD_NAME_PROTO_INDEX_MAPPING", String.format("{\"%d\":\"feedbackOrderNumber\", \"%d\":\"comment\" , \"%d\":\"tipAmount\"}", feedbackOrderNumberIndex, feedbackCommentIndex, tipAmountIndex));

    }

    private void setupTagNameIndexMappingProperties() {
        props.setProperty("TAG_NAME_PROTO_INDEX_MAPPING", String.format("{\"%d\":\"driver_id\"}", driverIdIndex));
    }

    public class InfluxSinkStub extends InfluxSink {
        public InfluxSinkStub(Instrumentation instrumentation, String sinkType, InfluxSinkConfig config, ProtoParser protoParser, InfluxDB client, StencilClient stencilClient) {
            super(instrumentation, sinkType, config, protoParser, client, stencilClient);
        }

        public void prepare(List<EsbMessage> messages) throws IOException {
            super.prepare(messages);
        }
    }

}
