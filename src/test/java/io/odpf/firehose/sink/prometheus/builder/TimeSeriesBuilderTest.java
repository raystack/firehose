package io.odpf.firehose.sink.prometheus.builder;

import io.odpf.firehose.config.PrometheusSinkConfig;
import io.odpf.firehose.consumer.TestDurationMessage;
import io.odpf.firehose.consumer.TestFeedbackLogMessage;
import io.odpf.firehose.exception.EglcConfigurationException;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import cortexpb.Cortex;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Properties;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TimeSeriesBuilderTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testMessageWithSingleMetric() throws InvalidProtocolBufferException {
        Properties promConfigProps = new Properties();

        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "true");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", "{\"7\": \"tip_amount\"}");
        promConfigProps.setProperty("SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING", "{ \"4\": \"customer_id\", \"13\": \"support_ticket_created\" }");

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setCustomerId("CUSTOMER")
                .setTipAmount(10000)
                .setSupportTicketCreated(true)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000)).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        PrometheusSinkConfig prometheusSinkConfig = ConfigFactory.create(PrometheusSinkConfig.class, promConfigProps);

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(prometheusSinkConfig)
                .buildTimeSeries(dynamicMessage, 2);

        String expectedResult = "[labels {\n  name: \"__name__\"\n  value: \"tip_amount\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nlabels {\n  name: \"support_ticket_created\"\n  value: \"true\"\n}\nlabels {\n  name: \"customer_id\"\n  value: \"CUSTOMER\"\n}\nsamples {\n  value: 10000.0\n  timestamp_ms: 1000000000\n}\n]";

        assertEquals(timeSeries.toString(), expectedResult);
    }

    @Test
    public void testMessageWithMultipleMetric() throws InvalidProtocolBufferException {
        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "true");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", " {\"7\": \"tip_amount\", \"5\": \"feedback_ratings\" }");
        promConfigProps.setProperty("SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING", " {\"3\": \"driver_id\" }");

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setDriverId("DRIVER")
                .setTipAmount(10000)
                .setFeedbackRating(5)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000)).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        PrometheusSinkConfig prometheusSinkConfig = ConfigFactory.create(PrometheusSinkConfig.class, promConfigProps);

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(prometheusSinkConfig)
                .buildTimeSeries(dynamicMessage, 2);

        String expectedResult = "[labels {\n  name: \"__name__\"\n  value: \"feedback_ratings\"\n}\nlabels {\n  name: \"driver_id\"\n  value: \"DRIVER\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nsamples {\n  value: 5.0\n  timestamp_ms: 1000000000\n}\n, labels {\n  name: \"__name__\"\n  value: \"tip_amount\"\n}\nlabels {\n  name: \"driver_id\"\n  value: \"DRIVER\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nsamples {\n  value: 10000.0\n  timestamp_ms: 1000000000\n}\n]";

        assertEquals(timeSeries.toString(), expectedResult);
    }

    @Test
    public void testMessageWithTimestampIsBuiltIntoMillis() throws InvalidProtocolBufferException {
        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "true");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", "{ \"7\": \"tip_amount\" }");
        promConfigProps.setProperty("SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING", "{ \"2\": \"event_timestamp\", \"3\": \"driver_id\" }");

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setDriverId("DRIVER")
                .setTipAmount(10000)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000)).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        PrometheusSinkConfig prometheusSinkConfig = ConfigFactory.create(PrometheusSinkConfig.class, promConfigProps);

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(prometheusSinkConfig)
                .buildTimeSeries(dynamicMessage, 2);

        String expectedResult = "[labels {\n  name: \"__name__\"\n  value: \"tip_amount\"\n}\nlabels {\n  name: \"driver_id\"\n  value: \"DRIVER\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nlabels {\n  name: \"event_timestamp\"\n  value: \"1000000000\"\n}\nsamples {\n  value: 10000.0\n  timestamp_ms: 1000000000\n}\n]";

        assertEquals(timeSeries.toString(), expectedResult);

    }

    @Test
    public void testMessageWithDurationIsBuiltIntoMillis() throws InvalidProtocolBufferException {
        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "4");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "true");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", "{ \"1\": \"order_number\" }");

        TestDurationMessage testDurationMessage = TestDurationMessage.newBuilder()
                .setOrderNumber("100")
                .setDuration(Duration.newBuilder().setSeconds(1000))
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestDurationMessage.getDescriptor(), testDurationMessage.toByteArray());

        PrometheusSinkConfig prometheusSinkConfig = ConfigFactory.create(PrometheusSinkConfig.class, promConfigProps);

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(prometheusSinkConfig)
                .buildTimeSeries(dynamicMessage, 2);

        String expectedResult = "[labels {\n  name: \"__name__\"\n  value: \"order_number\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nsamples {\n  value: 100.0\n  timestamp_ms: 1000000\n}\n]";

        assertEquals(timeSeries.toString(), expectedResult);
    }

    @Test
    public void testMessageWithEnum() throws InvalidProtocolBufferException {
        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "true");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", "{ \"7\": \"tip_amount\" }");
        promConfigProps.setProperty("SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING", "{ \"10\": \"feedback_source\" }");

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setTipAmount(10000)
                .setFeedbackSource(io.odpf.firehose.consumer.TestFeedbackSource.Enum.DRIVER)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000))
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        PrometheusSinkConfig prometheusSinkConfig = ConfigFactory.create(PrometheusSinkConfig.class, promConfigProps);

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(prometheusSinkConfig)
                .buildTimeSeries(dynamicMessage, 2);

        String expectedResult = "[labels {\n  name: \"__name__\"\n  value: \"tip_amount\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nlabels {\n  name: \"feedback_source\"\n  value: \"DRIVER\"\n}\nsamples {\n  value: 10000.0\n  timestamp_ms: 1000000000\n}\n]";

        assertEquals(timeSeries.toString(), expectedResult);
    }

    @Test
    public void testMessageWithNestedSchemaForMetrics() throws InvalidProtocolBufferException {
        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "true");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", "{ \"15\": { \"1\": \"order_completion_time_seconds\" },  \"7\": \"tip_amount\"}");
        promConfigProps.setProperty("SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING", "{ \"3\": \"driver_id\" }");

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setTipAmount(100)
                .setDriverId("DRIVER")
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000))
                .setOrderCompletionTime(Timestamp.newBuilder().setSeconds(12345))
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        PrometheusSinkConfig prometheusSinkConfig = ConfigFactory.create(PrometheusSinkConfig.class, promConfigProps);

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(prometheusSinkConfig)
                .buildTimeSeries(dynamicMessage, 2);

        String expectedResult = "[labels {\n  name: \"__name__\"\n  value: \"order_completion_time_seconds\"\n}\nlabels {\n  name: \"driver_id\"\n  value: \"DRIVER\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nsamples {\n  value: 12345.0\n  timestamp_ms: 1000000000\n}\n]";

        assertEquals(timeSeries.toString(), expectedResult);
    }

    @Test
    public void testMessageWithNestedSchemaForLabels() throws InvalidProtocolBufferException {
        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "true");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", "{ \"7\": \"tip_amount\" }");
        promConfigProps.setProperty("SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING", "{ \"15\": { \"1\": \"order_completion_time_seconds\" } }");

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setTipAmount(10000)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000))
                .setOrderCompletionTime(Timestamp.newBuilder().setSeconds(12345))
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        PrometheusSinkConfig prometheusSinkConfig = ConfigFactory.create(PrometheusSinkConfig.class, promConfigProps);

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(prometheusSinkConfig)
                .buildTimeSeries(dynamicMessage, 2);

        String expectedResult = "[labels {\n  name: \"__name__\"\n  value: \"tip_amount\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nlabels {\n  name: \"order_completion_time_seconds\"\n  value: \"12345\"\n}\nsamples {\n  value: 10000.0\n  timestamp_ms: 1000000000\n}\n]";

        assertEquals(timeSeries.toString(), expectedResult);
    }

    @Test
    public void testMessageWithMetricTimestampUsingIngestionTimestamp() throws InvalidProtocolBufferException {
        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "false");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", "{ \"1\": \"order_number\" }");
        promConfigProps.setProperty("SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING", "{ \"3\": \"driver_id\" }");

        TestFeedbackLogMessage testFeedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setOrderNumber("100")
                .setDriverId("DRIVER")
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000))
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), testFeedbackLogMessage.toByteArray());

        PrometheusSinkConfig prometheusSinkConfig = ConfigFactory.create(PrometheusSinkConfig.class, promConfigProps);

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(prometheusSinkConfig)
                .buildTimeSeries(dynamicMessage, 2);

        String eventTimestampResult = "[labels {\n  name: \"__name__\"\n  value: \"order_number\"\n}\n\nlabels {\n  name: \"driver_id\"\n  value: \"DRIVER\"\n}labels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nsamples {\n  value: 100.0\n  timestamp_ms: 1000000\n}\n]";

        assertNotEquals(timeSeries.toString(), eventTimestampResult);
    }

    @Test
    public void testMessageWithEmptyMetricProtoMappingConfig() throws InvalidProtocolBufferException {
        expectedEx.expect(EglcConfigurationException.class);
        expectedEx.expectMessage("field index mapping cannot be empty; at least one field value is required");

        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "false");
        promConfigProps.setProperty("SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING", "{ \"3\": \"driver_id\" }");

        TestFeedbackLogMessage testFeedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setOrderNumber("100")
                .setDriverId("DRIVER")
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000))
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), testFeedbackLogMessage.toByteArray());

        PrometheusSinkConfig prometheusSinkConfig = ConfigFactory.create(PrometheusSinkConfig.class, promConfigProps);

        TimeSeriesBuilder timeSeries = new TimeSeriesBuilder(prometheusSinkConfig);
        timeSeries.buildTimeSeries(dynamicMessage, 2);

    }

    @Test
    public void testMessageWithEmptyLabelProtoMappingConfig() throws InvalidProtocolBufferException {
        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "true");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", "{ \"7\": \"tip_amount\" }");

        TestFeedbackLogMessage testFeedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setTipAmount(12345)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000))
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), testFeedbackLogMessage.toByteArray());

        PrometheusSinkConfig prometheusSinkConfig = ConfigFactory.create(PrometheusSinkConfig.class, promConfigProps);

        List<Cortex.TimeSeries> timeSeries = new TimeSeriesBuilder(prometheusSinkConfig)
                .buildTimeSeries(dynamicMessage, 2);

        String expectedResult = "[labels {\n  name: \"__name__\"\n  value: \"tip_amount\"\n}\nlabels {\n  name: \"kafka_partition\"\n  value: \"2\"\n}\nsamples {\n  value: 12345.0\n  timestamp_ms: 1000000\n}\n]";

        assertEquals(timeSeries.toString(), expectedResult);

    }

    @Test
    public void testMessageWithEmptyFieldIndex() throws InvalidProtocolBufferException {
        expectedEx.expect(EglcConfigurationException.class);
        expectedEx.expectMessage("field index mapping cannot be empty; at least one field value is required");

        Properties promConfigProps = new Properties();
        promConfigProps.setProperty("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX", "2");
        promConfigProps.setProperty("INPUT_SCHEMA_PROTO_CLASS", TestFeedbackLogMessage.class.getName());
        promConfigProps.setProperty("SINK_PROM_WITH_EVENT_TIMESTAMP", "false");
        promConfigProps.setProperty("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING", "{}");

        TestFeedbackLogMessage testFeedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setDriverId("DRIVER")
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000))
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), testFeedbackLogMessage.toByteArray());

        PrometheusSinkConfig prometheusSinkConfig = ConfigFactory.create(PrometheusSinkConfig.class, promConfigProps);

        TimeSeriesBuilder timeSeries = new TimeSeriesBuilder(prometheusSinkConfig);
        timeSeries.buildTimeSeries(dynamicMessage, 2);
    }
}
