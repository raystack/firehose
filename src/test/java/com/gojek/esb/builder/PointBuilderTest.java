package com.gojek.esb.builder;

import com.gojek.esb.config.InfluxSinkConfig;
import com.gojek.esb.consumer.TestDurationMessage;
import com.gojek.esb.feedback.FeedbackLogMessage;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import org.aeonbits.owner.ConfigFactory;
import org.influxdb.dto.Point;
import org.junit.Test;

import java.util.Properties;

public class PointBuilderTest {

    @Test
    public void testMessageWithTimestampIsBuiltIntoMillis() throws InvalidProtocolBufferException {
        Properties influxConfigProps = new Properties();
        influxConfigProps.setProperty("MEASUREMENT_NAME", "test_point_builder");
        influxConfigProps.setProperty("PROTO_EVENT_TIMESTAMP_INDEX", "2");
        influxConfigProps.setProperty("DATABASE_NAME", "test");
        influxConfigProps.setProperty("PROTO_SCHEMA", FeedbackLogMessage.class.getName());
        influxConfigProps.setProperty("FIELD_NAME_PROTO_INDEX_MAPPING", "{ \"2\": \"event_timestamp\", \"7\": \"tip_amount\" }");
        influxConfigProps.setProperty("TAG_NAME_PROTO_INDEX_MAPPING", "{ \"4\": \"customer_id\", \"3\": \"driver_id\" }");

        FeedbackLogMessage feedbackLogMessage = FeedbackLogMessage.newBuilder()
                .setCustomerId("CUSTOMER")
                .setDriverId("DRIVER")
                .setTipAmount(10000)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000)).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(FeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        InfluxSinkConfig influxSinkConfig = ConfigFactory.create(InfluxSinkConfig.class, influxConfigProps);

        Point point = new PointBuilder(influxSinkConfig)
                .buildPoint(dynamicMessage);

        assert point.lineProtocol().equals("test_point_builder,customer_id=CUSTOMER,driver_id=DRIVER event_timestamp=1000000000i,tip_amount=10000.0 1000000000000000");
    }

    @Test
    public void testMessageWithEnum() throws InvalidProtocolBufferException {
        Properties influxConfigProps = new Properties();
        influxConfigProps.setProperty("MEASUREMENT_NAME", "test_point_builder");
        influxConfigProps.setProperty("PROTO_EVENT_TIMESTAMP_INDEX", "2");
        influxConfigProps.setProperty("DATABASE_NAME", "test");
        influxConfigProps.setProperty("PROTO_SCHEMA", FeedbackLogMessage.class.getName());
        influxConfigProps.setProperty("FIELD_NAME_PROTO_INDEX_MAPPING", "{ \"10\": \"feedback_source\"}");
        influxConfigProps.setProperty("TAG_NAME_PROTO_INDEX_MAPPING", "{ \"1\": \"order_number\" }");

        FeedbackLogMessage feedbackLogMessage = FeedbackLogMessage.newBuilder()
                .setOrderNumber("ORDER")
                .setFeedbackSource(com.gojek.esb.feedback.FeedbackSource.Enum.DRIVER)
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(FeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        InfluxSinkConfig influxSinkConfig = ConfigFactory.create(InfluxSinkConfig.class, influxConfigProps);

        Point point = new PointBuilder(influxSinkConfig)
                .buildPoint(dynamicMessage);

        assert point.lineProtocol().equals("test_point_builder,order_number=ORDER feedback_source=\"DRIVER\" 0");

    }

    @Test
    public void testMessageWithDurationIsBuiltIntoMillis() throws InvalidProtocolBufferException {
        Properties influxConfigProps = new Properties();
        influxConfigProps.setProperty("MEASUREMENT_NAME", "test_point_builder");
        influxConfigProps.setProperty("PROTO_EVENT_TIMESTAMP_INDEX", "5");
        influxConfigProps.setProperty("DATABASE_NAME", "test");
        influxConfigProps.setProperty("PROTO_SCHEMA", FeedbackLogMessage.class.getName());
        influxConfigProps.setProperty("FIELD_NAME_PROTO_INDEX_MAPPING", "{ \"4\": \"duration\" }");
        influxConfigProps.setProperty("TAG_NAME_PROTO_INDEX_MAPPING", "{ \"1\": \"order_number\" }");

        TestDurationMessage testDurationMessage = TestDurationMessage.newBuilder()
                .setOrderNumber("ORDER")
                .setDuration(Duration.newBuilder().setSeconds(1000))
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000))
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestDurationMessage.getDescriptor(), testDurationMessage.toByteArray());

        InfluxSinkConfig influxSinkConfig = ConfigFactory.create(InfluxSinkConfig.class, influxConfigProps);

        Point point = new PointBuilder(influxSinkConfig)
                .buildPoint(dynamicMessage);

        assert point.lineProtocol().equals("test_point_builder,order_number=ORDER duration=1000000i 1000000000000000");
    }

    @Test
    public void testMessageWithNestedSchemaForFields() throws InvalidProtocolBufferException {
        Properties influxConfigProps = new Properties();
        influxConfigProps.setProperty("MEASUREMENT_NAME", "test_point_builder");
        influxConfigProps.setProperty("PROTO_EVENT_TIMESTAMP_INDEX", "2");
        influxConfigProps.setProperty("DATABASE_NAME", "test");
        influxConfigProps.setProperty("PROTO_SCHEMA", FeedbackLogMessage.class.getName());
        influxConfigProps.setProperty("FIELD_NAME_PROTO_INDEX_MAPPING", "{ \"2\": \"event_timestamp\", \"7\": \"tip_amount\", \"15\": { \"1\": \"order_completion_time_seconds\" } }");
        influxConfigProps.setProperty("TAG_NAME_PROTO_INDEX_MAPPING", "{ \"4\": \"customer_id\", \"3\": \"driver_id\" }");

        FeedbackLogMessage feedbackLogMessage = FeedbackLogMessage.newBuilder()
                .setCustomerId("CUSTOMER")
                .setDriverId("DRIVER")
                .setTipAmount(10000)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000))
                .setOrderCompletionTime(Timestamp.newBuilder().setSeconds(12345))
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(FeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        InfluxSinkConfig influxSinkConfig = ConfigFactory.create(InfluxSinkConfig.class, influxConfigProps);

        Point point = new PointBuilder(influxSinkConfig)
                .buildPoint(dynamicMessage);

        assert point.lineProtocol().equals("test_point_builder,customer_id=CUSTOMER,driver_id=DRIVER event_timestamp=1000000000i,order_completion_time_seconds=12345i,tip_amount=10000.0 1000000000000000");
    }

    @Test
    public void testMessageWithNestedSchemaForTags() throws InvalidProtocolBufferException {
        Properties influxConfigProps = new Properties();
        influxConfigProps.setProperty("MEASUREMENT_NAME", "test_point_builder");
        influxConfigProps.setProperty("PROTO_EVENT_TIMESTAMP_INDEX", "2");
        influxConfigProps.setProperty("DATABASE_NAME", "test");
        influxConfigProps.setProperty("PROTO_SCHEMA", FeedbackLogMessage.class.getName());
        influxConfigProps.setProperty("FIELD_NAME_PROTO_INDEX_MAPPING", "{ \"2\": \"event_timestamp\", \"7\": \"tip_amount\" }");
        influxConfigProps.setProperty("TAG_NAME_PROTO_INDEX_MAPPING", "{ \"4\": \"customer_id\", \"3\": \"driver_id\", \"15\": { \"1\": \"order_completion_time_seconds\" } }");

        FeedbackLogMessage feedbackLogMessage = FeedbackLogMessage.newBuilder()
                .setCustomerId("CUSTOMER")
                .setDriverId("DRIVER")
                .setTipAmount(10000)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000))
                .setOrderCompletionTime(Timestamp.newBuilder().setSeconds(12345))
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(FeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        InfluxSinkConfig influxSinkConfig = ConfigFactory.create(InfluxSinkConfig.class, influxConfigProps);

        Point point = new PointBuilder(influxSinkConfig)
                .buildPoint(dynamicMessage);

        assert point.lineProtocol().equals("test_point_builder,customer_id=CUSTOMER,driver_id=DRIVER,order_completion_time_seconds=12345 event_timestamp=1000000000i,tip_amount=10000.0 1000000000000000");
    }
}
