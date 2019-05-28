package com.gojek.esb.builder;

import com.gojek.esb.config.InfluxSinkConfig;
import com.gojek.esb.feedback.FeedbackLogMessage;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import org.aeonbits.owner.ConfigFactory;
import org.influxdb.dto.Point;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Properties;

@RunWith(MockitoJUnitRunner.class)
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
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000).build()).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(FeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());

        InfluxSinkConfig influxSinkConfig = ConfigFactory.create(InfluxSinkConfig.class, influxConfigProps);

        Point point = new PointBuilder(influxSinkConfig)
                .buildPoint(dynamicMessage);

        assert point.lineProtocol().equals("test_point_builder,customer_id=CUSTOMER,driver_id=DRIVER event_timestamp=1000000000i,tip_amount=10000.0 1000000000000000");
    }
}