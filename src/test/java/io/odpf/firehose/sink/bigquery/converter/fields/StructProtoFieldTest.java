package io.odpf.firehose.sink.bigquery.converter.fields;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.odpf.firehose.consumer.TestStructMessage;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StructProtoFieldTest {

    private StructProtoField structProtoField;
    private Struct structValue;

    @Before
    public void setUp() throws Exception {
        List<Value> listValues = new ArrayList<>();
        listValues.add(Value.newBuilder().setNumberValue(1).build());
        listValues.add(Value.newBuilder().setNumberValue(2).build());

        structValue = Struct.newBuilder()
                .putFields("null_value", Value.newBuilder().setNullValue(NullValue.NULL_VALUE)
                        .build())
                .putFields("number_value", Value.newBuilder().setNumberValue(2.0).build())
                .putFields("string_value", Value.newBuilder().setStringValue("").build())
                .putFields("bool_value", Value.newBuilder().setBoolValue(false).build())
                .putFields("struct_value", Value.newBuilder().setStructValue(
                        Struct.newBuilder().putFields("child_value1", Value.newBuilder().setNumberValue(1.0).build())
                                .build())
                        .build())
                .putFields("list_value", Value.newBuilder().setListValue(ListValue.newBuilder()
                        .addAllValues(listValues).build()).build())
                .build();
        TestStructMessage message = TestStructMessage.newBuilder()
                .setOrderNumber("123X")
                .setCustomFields(structValue)
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray());
        Descriptors.FieldDescriptor customValues = dynamicMessage.getDescriptorForType().findFieldByName("custom_fields");
        structProtoField = new StructProtoField(customValues, dynamicMessage.getField(customValues));
    }

    @Test
    public void shouldSerialiseStructIntoJson() {
        String value = (String) structProtoField.getValue();
        String jsonStr = "{\"null_value\":null,"
                + "\"number_value\":2.0,"
                + "\"string_value\":\"\","
                + "\"bool_value\":false,"
                + "\"struct_value\":{\"child_value1\":1.0},"
                + "\"list_value\":[1.0,2.0]}";

        assertEquals(jsonStr, value);
    }

    @Test
    public void shouldSerialiseRepeatedStructsIntoJson() throws InvalidProtocolBufferException {
        Struct simpleStruct = Struct.newBuilder()
                .putFields("null_value", Value.newBuilder().setNullValue(NullValue.NULL_VALUE)
                        .build())
                .putFields("number_value", Value.newBuilder().setNumberValue(2.0).build())
                .build();

        TestStructMessage message = TestStructMessage.newBuilder()
                .setOrderNumber("123X")
                .addListCustomFields(simpleStruct)
                .addListCustomFields(simpleStruct)
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray());
        Descriptors.FieldDescriptor listCustomFieldsDescriptor = dynamicMessage.getDescriptorForType().findFieldByName("list_custom_fields");
        structProtoField = new StructProtoField(listCustomFieldsDescriptor, dynamicMessage.getField(listCustomFieldsDescriptor));

        Object value = structProtoField.getValue();

        List<String> jsonStrList = new ArrayList<>();
        jsonStrList.add("{\"null_value\":null,\"number_value\":2.0}");
        jsonStrList.add("{\"null_value\":null,\"number_value\":2.0}");

        assertEquals(jsonStrList, value);
    }

    @Test
    public void shouldMatchStruct() {
        boolean isMatch = structProtoField.matches();
        assertTrue(isMatch);
    }
}
