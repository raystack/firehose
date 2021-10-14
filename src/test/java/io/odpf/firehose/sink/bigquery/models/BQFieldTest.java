package io.odpf.firehose.sink.bigquery.models;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.protobuf.Descriptors;
import io.odpf.firehose.consumer.TestMessage;
import io.odpf.firehose.consumer.TestTypesMessage;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class BQFieldTest {

    private Descriptors.Descriptor testMessageDescriptor = TestTypesMessage.newBuilder().build().getDescriptorForType();

    @Test
    public void shouldReturnBigqueryField() {
        String fieldName = "double_value";
        Field expected = Field.newBuilder(fieldName, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build();
        Field field = fieldDescriptorToField(testMessageDescriptor.findFieldByName(fieldName));

        assertEquals(expected, field);
    }

    @Test
    public void shouldReturnBigqueryFieldWithChildField() {
        String fieldName = "message_value";

        TestMessage testMessage = TestMessage.newBuilder().build();
        Descriptors.FieldDescriptor orderNumber = testMessage.getDescriptorForType().findFieldByName("order_number");
        Descriptors.FieldDescriptor orderUrl = testMessage.getDescriptorForType().findFieldByName("order_url");
        Descriptors.FieldDescriptor orderDetails = testMessage.getDescriptorForType().findFieldByName("order_details");

        List<Field> childFields = new ArrayList<>();
        childFields.add(fieldDescriptorToField(orderNumber));
        childFields.add(fieldDescriptorToField(orderUrl));
        childFields.add(fieldDescriptorToField(orderDetails));

        Descriptors.FieldDescriptor messageFieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField bqField = fieldDescriptorToBQField(messageFieldDescriptor);
        bqField.setSubFields(childFields);
        Field field = bqField.getField();

        Field expectedOrderNumberBqField = Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build();
        Field expectedOrderNumberBqFieldUrl = Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build();
        Field expectedOrderDetailsBqField1 = Field.newBuilder("order_details", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build();

        Field expected = Field.newBuilder(fieldName, LegacySQLTypeName.RECORD,
                FieldList.of(expectedOrderNumberBqField,
                        expectedOrderNumberBqFieldUrl,
                        expectedOrderDetailsBqField1)).setMode(Field.Mode.NULLABLE).build();

        assertEquals(expected, field);
    }


    @Test
    public void shouldConvertProtobufTimestampToBigqueryTimestamp() {
        String fieldName = "timestamp_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);

        BQField bqField = fieldDescriptorToBQField(fieldDescriptor);
        LegacySQLTypeName bqFieldType = bqField.getType();

        assertEquals(LegacySQLTypeName.TIMESTAMP, bqFieldType);
    }

    @Test
    public void shouldConvertProtobufStructToBigqueryString() {
        String fieldName = "struct_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);

        BQField bqField = fieldDescriptorToBQField(fieldDescriptor);
        LegacySQLTypeName bqFieldType = bqField.getType();

        assertEquals(LegacySQLTypeName.STRING, bqFieldType);
    }

    @Test
    public void shouldConvertProtobufDurationToBigqueryRecord() {
        String fieldName = "duration_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField bqField = fieldDescriptorToBQField(fieldDescriptor);
        LegacySQLTypeName bqFieldType = bqField.getType();

        assertEquals(LegacySQLTypeName.RECORD, bqFieldType);
    }


    @Test
    public void shouldConvertProtobufDoubleToBigqueryFloat() {
        String fieldName = "double_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.FLOAT, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    @Test
    public void shouldConvertProtobufFloatToBigqueryFloat() {
        String fieldName = "float_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.FLOAT, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    @Test
    public void shouldConvertProtobufBytesToBigqueryBytes() {
        String fieldName = "bytes_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.BYTES, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    @Test
    public void shouldConvertProtobufBoolToBigqueryBool() {
        String fieldName = "bool_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.BOOLEAN, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    @Test
    public void shouldConvertProtobufEnumToBigqueryString() {
        String fieldName = "enum_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.STRING, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    @Test
    public void shouldConvertProtobufStringToBigqueryString() {
        String fieldName = "string_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.STRING, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    @Test
    public void shouldConvertProtobufInt64ToBigqueryInteger() {
        String fieldName = "int64_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.INTEGER, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    @Test
    public void shouldConvertProtobufUint64ToBigqueryInteger() {
        String fieldName = "uint64_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.INTEGER, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    @Test
    public void shouldConvertProtobufInt32ToBigqueryInteger() {
        String fieldName = "int32_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);

        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.INTEGER, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    @Test
    public void shouldConvertProtobufUint32ToBigqueryInteger() {
        String fieldName = "uint32_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.INTEGER, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    @Test
    public void shouldConvertProtobufFixed32ToBigqueryInteger() {
        String fieldName = "fixed32_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.INTEGER, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    @Test
    public void shouldConvertProtobufFixed64ToBigqueryInteger() {
        String fieldName = "fixed64_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.INTEGER, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    @Test
    public void shouldConvertProtobufSfixed32ToBigqueryInteger() {
        String fieldName = "sfixed32_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.INTEGER, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    @Test
    public void shouldConvertProtobufSfixed64ToBigqueryInteger() {
        String fieldName = "sfixed32_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.INTEGER, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    @Test
    public void shouldConvertProtobufSint32ToBigqueryInteger() {
        String fieldName = "sint32_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.INTEGER, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    @Test
    public void shouldConvertProtobufSint64ToBigqueryInteger() {
        String fieldName = "sint64_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.INTEGER, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    @Test
    public void shouldConvertProtobufMessageTypeToBigqueryRecord() {
        String fieldName = "message_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.RECORD, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    @Test
    public void shouldConvertProtobufRepeatedModifierToBigqueryRepeatedColumn() {
        String fieldName = "list_values";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.REPEATED, LegacySQLTypeName.STRING, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    @Test
    public void shouldConvertProtobufRepeatedMessageModifierToBigqueryRepeatedRecordColumn() {
        String fieldName = "list_message_values";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.REPEATED, LegacySQLTypeName.RECORD, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    private Field fieldDescriptorToField(Descriptors.FieldDescriptor fieldDescriptor) {
        BQField bqField = fieldDescriptorToBQField(fieldDescriptor);
        return bqField.getField();
    }

    private BQField fieldDescriptorToBQField(Descriptors.FieldDescriptor fieldDescriptor) {
        ProtoField protoField = new ProtoField(fieldDescriptor.toProto());
        return new BQField(protoField);
    }

}
