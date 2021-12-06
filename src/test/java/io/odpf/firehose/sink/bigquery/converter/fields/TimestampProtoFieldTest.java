package io.odpf.firehose.sink.bigquery.converter.fields;

import com.google.api.client.util.DateTime;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import io.odpf.firehose.consumer.TestDurationMessage;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.*;

public class TimestampProtoFieldTest {
    private TimestampProtoField timestampProtoField;
    private Instant time;

    @Before
    public void setUp() throws Exception {
        time = Instant.ofEpochSecond(200, 200);
        TestDurationMessage message = TestDurationMessage.newBuilder()
                .setEventTimestamp(Timestamp.newBuilder()
                        .setSeconds(time.getEpochSecond())
                        .setNanos(time.getNano())
                        .build())
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray());
        Descriptors.FieldDescriptor fieldDescriptor = dynamicMessage.getDescriptorForType().findFieldByName("event_timestamp");
        timestampProtoField = new TimestampProtoField(fieldDescriptor, dynamicMessage.getField(fieldDescriptor));
    }

    @Test
    public void shouldParseGoogleProtobufTimestampProtoMessageToDateTime() throws InvalidProtocolBufferException {
        DateTime dateTimeResult = (DateTime) timestampProtoField.getValue();

        DateTime expected = new DateTime(time.toEpochMilli());
        assertEquals(expected, dateTimeResult);
    }

    @Test
    public void shouldMatchGoogleProtobufTimestamp() {
        assertTrue(timestampProtoField.matches());
    }
}
