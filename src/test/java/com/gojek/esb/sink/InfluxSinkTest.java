package com.gojek.esb.sink;

import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.InfluxSinkConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.exception.EglcConfigurationException;
import com.gojek.esb.feedback.FeedbackLogKey;
import com.gojek.esb.feedback.FeedbackLogMessage;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.influxdb.InfluxSink;
import com.gojek.esb.util.Clock;
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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class InfluxSinkTest {
    private Sink sink;
    private EsbMessage esbMessage;
    private Point expectedPoint;
    private InfluxSinkConfig config;
    private Properties props = new Properties();
    private Point.Builder pointBuilder;
    private StencilClient stencilClient;

    private final String measurementName = "FeedbackMeasure";
    private static final int TIMESTAMP_IN_EPOCH_SECONDS = 1498600;
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

    @Mock
    private InfluxDB client;

    @Mock
    private StatsDReporter statsDReporter;

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
        pointBuilder = Point.measurement(measurementName).addField("feedbackOrderNumber", orderNumber)
                .addField("comment", feedbackComment).addField("tipAmount", tipAmount).time(TIMESTAMP_IN_EPOCH_SECONDS, TimeUnit.SECONDS);
        stencilClient = StencilClientFactory.getClient();
        when(statsDReporter.getClock()).thenReturn(new Clock());
    }

    @Test
    public void shouldPushTagsAsStringValues() throws DeserializerException, IOException {
        expectedPoint = pointBuilder.tag("driver_id", driverId).build();
        setupFieldNameIndexMappingProperties();
        setupTagNameIndexMappingProperties();
        config = ConfigFactory.create(InfluxSinkConfig.class, props);

        sink = new InfluxSink(client, new ProtoParser(stencilClient, config.getProtoSchema()), config, statsDReporter, stencilClient);

        ArgumentCaptor<BatchPoints> batchPointsArgumentCaptor = ArgumentCaptor.forClass(BatchPoints.class);

        sink.pushMessage(Arrays.asList(esbMessage));
        verify(client, times(1)).write(batchPointsArgumentCaptor.capture());
        List<BatchPoints> batchPointsList = batchPointsArgumentCaptor.getAllValues();

        assertEquals(expectedPoint.lineProtocol(), batchPointsList.get(0).getPoints().get(0).lineProtocol());
    }

    @Test
    public void shouldThrowExceptionOnEmptyFieldNameIndexMapping() throws IOException, DeserializerException {
        props.setProperty("FIELD_NAME_PROTO_INDEX_MAPPING", emptyFieldNameIndex);
        props.setProperty("TAG_NAME_PROTO_INDEX_MAPPING", emptyTagNameIndexMapping);
        config = ConfigFactory.create(InfluxSinkConfig.class, props);
        sink = new InfluxSink(client, new ProtoParser(stencilClient, config.getProtoSchema()), config, statsDReporter, stencilClient);

        try {
            sink.pushMessage(Arrays.asList(esbMessage));

            fail("should throw a eglc configuration exception on fieldname index mapping being empty");
        } catch (EglcConfigurationException e) {
            assertEquals(InfluxSink.FIELD_NAME_MAPPING_ERROR_MESSAGE, e.getMessage());
        }
    }

    @Test
    public void shouldPushMessagesWithType() throws DeserializerException, IOException {
        expectedPoint = pointBuilder.build();
        setupFieldNameIndexMappingProperties();
        props.setProperty("TAG_NAME_PROTO_INDEX_MAPPING", emptyTagNameIndexMapping);
        config = ConfigFactory.create(InfluxSinkConfig.class, props);
        sink = new InfluxSink(client, new ProtoParser(stencilClient, config.getProtoSchema()), config, statsDReporter, stencilClient);
        ArgumentCaptor<BatchPoints> batchPointsArgumentCaptor = ArgumentCaptor.forClass(BatchPoints.class);

        sink.pushMessage(Arrays.asList(esbMessage));
        verify(client, times(1)).write(batchPointsArgumentCaptor.capture());
        List<BatchPoints> batchPointsList = batchPointsArgumentCaptor.getAllValues();

        assertEquals(expectedPoint.lineProtocol(), batchPointsList.get(0).getPoints().get(0).lineProtocol());
        verify(statsDReporter, times(1)).captureCount(any(), any(), any());
        verify(statsDReporter, times(2)).captureDurationSince(any(), any(), any());

    }

    private void setupFieldNameIndexMappingProperties() {
        props.setProperty("FIELD_NAME_PROTO_INDEX_MAPPING", String.format("{\"%d\":\"feedbackOrderNumber\", \"%d\":\"comment\" , \"%d\":\"tipAmount\"}", feedbackOrderNumberIndex, feedbackCommentIndex, tipAmountIndex));

    }

    private void setupTagNameIndexMappingProperties() {
        props.setProperty("TAG_NAME_PROTO_INDEX_MAPPING", String.format("{\"%d\":\"driver_id\"}", driverIdIndex));
    }
}
