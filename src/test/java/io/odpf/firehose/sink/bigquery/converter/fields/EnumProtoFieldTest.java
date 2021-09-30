package io.odpf.firehose.sink.bigquery.converter.fields;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.firehose.consumer.TestEnumMessage;
import io.odpf.firehose.consumer.TestStatus;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EnumProtoFieldTest {

    private EnumProtoField enumProtoField;

    @Before
    public void setUp() throws Exception {
        TestEnumMessage testEnumMessage = TestEnumMessage.newBuilder().setLastStatus(TestStatus.Enum.CREATED).build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(testEnumMessage.getDescriptorForType(), testEnumMessage.toByteArray());
        Descriptors.FieldDescriptor fieldDescriptor = dynamicMessage.getDescriptorForType().findFieldByName("last_status");
        enumProtoField = new EnumProtoField(fieldDescriptor, dynamicMessage.getField(fieldDescriptor));
    }

    @Test
    public void shouldConvertProtobufEnumToString() {
        String fieldValue = (String) enumProtoField.getValue();
        assertEquals("CREATED", fieldValue);
    }

    @Test
    public void shouldConvertRepeatedProtobufEnumToListOfString() throws InvalidProtocolBufferException {
        TestEnumMessage testEnumMessage = TestEnumMessage.newBuilder()
                .addStatusHistory(TestStatus.Enum.CREATED)
                .addStatusHistory(TestStatus.Enum.IN_PROGRESS)
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(testEnumMessage.getDescriptorForType(), testEnumMessage.toByteArray());
        Descriptors.FieldDescriptor fieldDescriptor = dynamicMessage.getDescriptorForType().findFieldByName("status_history");
        enumProtoField = new EnumProtoField(fieldDescriptor, dynamicMessage.getField(fieldDescriptor));
        Object fieldValue = enumProtoField.getValue();

        ArrayList<String> enumValueList = new ArrayList<>();
        enumValueList.add("CREATED");
        enumValueList.add("IN_PROGRESS");
        assertEquals(enumValueList, fieldValue);
    }

    @Test
    public void shouldMatchEnumProtobufField() {
        boolean isMatch = enumProtoField.matches();
        assertTrue(isMatch);
    }
}
