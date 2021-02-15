package com.gojek.esb.builder;

import com.gojek.esb.config.InfluxSinkConfig;
import com.gojek.esb.consumer.TestDurationMessage;
import com.gojek.esb.consumer.TestFeedbackLogMessage;
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
        influxConfigProps.setProperty("sink.influx.measurement.name", "test_point_builder");
        influxConfigProps.setProperty("sink.influx.proto.event.timestamp.index", "2");
        influxConfigProps.setProperty("sink.influx.db.name", "test");
        influxConfigProps.setProperty("proto.schema", TestFeedbackLogMessage.class.getName());
        influxConfigProps.setProperty("sink.influx.field.name.proto.index.mapping", "{ \"2\": \"event_timestamp\", \"7\": \"tip_amount\" }");
        influxConfigProps.setProperty("sink.influx.tag.name.proto.index.mapping", "{ \"4\": \"customer_id\", \"3\": \"driver_id\" }");

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setCustomerId("CUSTOMER")
                .setDriverId("DRIVER")
                .setTipAmount(10000)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000)).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        InfluxSinkConfig influxSinkConfig = ConfigFactory.create(InfluxSinkConfig.class, influxConfigProps);

        Point point = new PointBuilder(influxSinkConfig)
                .buildPoint(dynamicMessage);

        assert point.lineProtocol().equals("test_point_builder,customer_id=CUSTOMER,driver_id=DRIVER event_timestamp=1000000000i,tip_amount=10000.0 1000000000000000");
    }

    @Test
    public void testMessageWithEnum() throws InvalidProtocolBufferException {
        Properties influxConfigProps = new Properties();
        influxConfigProps.setProperty("sink.influx.measurement.name", "test_point_builder");
        influxConfigProps.setProperty("sink.influx.proto.event.timestamp.index", "2");
        influxConfigProps.setProperty("sink.influx.db.name", "test");
        influxConfigProps.setProperty("proto.schema", TestFeedbackLogMessage.class.getName());
        influxConfigProps.setProperty("sink.influx.field.name.proto.index.mapping", "{ \"10\": \"feedback_source\"}");
        influxConfigProps.setProperty("sink.influx.tag.name.proto.index.mapping", "{ \"1\": \"order_number\" }");

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setOrderNumber("ORDER")
                .setFeedbackSource(com.gojek.esb.consumer.TestFeedbackSource.Enum.DRIVER)
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        InfluxSinkConfig influxSinkConfig = ConfigFactory.create(InfluxSinkConfig.class, influxConfigProps);

        Point point = new PointBuilder(influxSinkConfig)
                .buildPoint(dynamicMessage);

        assert point.lineProtocol().equals("test_point_builder,order_number=ORDER feedback_source=\"DRIVER\" 0");

    }

    @Test
    public void testMessageWithDurationIsBuiltIntoMillis() throws InvalidProtocolBufferException {
        Properties influxConfigProps = new Properties();
        influxConfigProps.setProperty("sink.influx.measurement.name", "test_point_builder");
        influxConfigProps.setProperty("sink.influx.proto.event.timestamp.index", "5");
        influxConfigProps.setProperty("sink.influx.db.name", "test");
        influxConfigProps.setProperty("proto.schema", TestFeedbackLogMessage.class.getName());
        influxConfigProps.setProperty("sink.influx.field.name.proto.index.mapping", "{ \"4\": \"duration\" }");
        influxConfigProps.setProperty("sink.influx.tag.name.proto.index.mapping", "{ \"1\": \"order_number\" }");

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
        influxConfigProps.setProperty("sink.influx.measurement.name", "test_point_builder");
        influxConfigProps.setProperty("sink.influx.proto.event.timestamp.index", "2");
        influxConfigProps.setProperty("sink.influx.db.name", "test");
        influxConfigProps.setProperty("proto.schema", TestFeedbackLogMessage.class.getName());
        influxConfigProps.setProperty("sink.influx.field.name.proto.index.mapping", "{ \"2\": \"event_timestamp\", \"7\": \"tip_amount\", \"15\": { \"1\": \"order_completion_time_seconds\" } }");
        influxConfigProps.setProperty("sink.influx.tag.name.proto.index.mapping", "{ \"4\": \"customer_id\", \"3\": \"driver_id\" }");

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setCustomerId("CUSTOMER")
                .setDriverId("DRIVER")
                .setTipAmount(10000)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000))
                .setOrderCompletionTime(Timestamp.newBuilder().setSeconds(12345))
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        InfluxSinkConfig influxSinkConfig = ConfigFactory.create(InfluxSinkConfig.class, influxConfigProps);

        Point point = new PointBuilder(influxSinkConfig)
                .buildPoint(dynamicMessage);

        assert point.lineProtocol().equals("test_point_builder,customer_id=CUSTOMER,driver_id=DRIVER event_timestamp=1000000000i,order_completion_time_seconds=12345i,tip_amount=10000.0 1000000000000000");
    }

    @Test
    public void testMessageWithNestedSchemaForTags() throws InvalidProtocolBufferException {
        Properties influxConfigProps = new Properties();
        influxConfigProps.setProperty("sink.influx.measurement.name", "test_point_builder");
        influxConfigProps.setProperty("sink.influx.proto.event.timestamp.index", "2");
        influxConfigProps.setProperty("sink.influx.db.name", "test");
        influxConfigProps.setProperty("proto.schema", TestFeedbackLogMessage.class.getName());
        influxConfigProps.setProperty("sink.influx.field.name.proto.index.mapping", "{ \"2\": \"event_timestamp\", \"7\": \"tip_amount\" }");
        influxConfigProps.setProperty("sink.influx.tag.name.proto.index.mapping", "{ \"4\": \"customer_id\", \"3\": \"driver_id\", \"15\": \"order_completion_time_seconds\" }");

        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setCustomerId("CUSTOMER")
                .setDriverId("DRIVER")
                .setTipAmount(10000)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000))
                .setOrderCompletionTime(Timestamp.newBuilder().setSeconds(12345))
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        InfluxSinkConfig influxSinkConfig = ConfigFactory.create(InfluxSinkConfig.class, influxConfigProps);

        Point point = new PointBuilder(influxSinkConfig)
                .buildPoint(dynamicMessage);

        assert point.lineProtocol().equals("test_point_builder,customer_id=CUSTOMER,driver_id=DRIVER,order_completion_time_seconds=12345000 event_timestamp=1000000000i,tip_amount=10000.0 1000000000000000");
    }
}
