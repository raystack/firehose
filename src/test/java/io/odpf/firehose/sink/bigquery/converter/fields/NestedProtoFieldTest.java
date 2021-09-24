package io.odpf.firehose.sink.bigquery.converter.fields;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.firehose.consumer.TestMessage;
import io.odpf.firehose.consumer.TestNestedMessage;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class NestedProtoFieldTest {

    private NestedProtoField nestedProtoField;
    private TestMessage childField;

    @Before
    public void setUp() throws Exception {
        childField = TestMessage.newBuilder()
                .setOrderNumber("123X")
                .build();
        TestNestedMessage nestedMessage = TestNestedMessage.newBuilder()
                .setSingleMessage(childField)
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(nestedMessage.getDescriptorForType(), nestedMessage.toByteArray());

        Descriptors.FieldDescriptor fieldDescriptor = nestedMessage.getDescriptorForType().findFieldByName("single_message");
        nestedProtoField = new NestedProtoField(fieldDescriptor, dynamicMessage.getField(fieldDescriptor));

    }

    @Test
    public void shouldReturnDynamicMessage() {
        DynamicMessage nestedChild = nestedProtoField.getValue();
        assertEquals(childField, nestedChild);
    }

    @Test
    public void shouldMatchDynamicMessageAsNested() {
        boolean isMatch = nestedProtoField.matches();
        assertTrue(isMatch);
    }
}
