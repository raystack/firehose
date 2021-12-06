package io.odpf.firehose.sink.bigquery.converter.fields;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.firehose.consumer.TestMessage;
import org.junit.Test;

import static org.junit.Assert.*;

public class DefaultProtoFieldTest {

    @Test
    public void shouldReturnProtobufElementsAsItIs() throws InvalidProtocolBufferException {
        String orderNumber = "123X";
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber(orderNumber).build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(testMessage.getDescriptorForType(), testMessage.toByteArray());
        Descriptors.FieldDescriptor fieldDescriptor = dynamicMessage.getDescriptorForType().findFieldByName("order_number");
        DefaultProtoField defaultProtoField = new DefaultProtoField(dynamicMessage.getField(fieldDescriptor));
        Object value = defaultProtoField.getValue();

        assertEquals(orderNumber, value);
    }

    @Test
    public void shouldNotMatchAnyType() {
        DefaultProtoField defaultProtoField = new DefaultProtoField(null);
        boolean isMatch = defaultProtoField.matches();
        assertFalse(isMatch);
    }
}
