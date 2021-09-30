package io.odpf.firehose.sink.bigquery.models;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import io.odpf.firehose.consumer.TestTypesMessage;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class ProtoFieldTest {
    @Test
    public void shouldReturnNestedAsTrueWhenProtobufFieldTypeIsAMessage() {
        DescriptorProtos.FieldDescriptorProto fieldDescriptorProto = TestTypesMessage.getDescriptor().findFieldByName("duration_value").toProto();
        ProtoField protoField = new ProtoField(fieldDescriptorProto);

        assertTrue(protoField.isNested());
    }

    @Test
    public void shouldReturnNestedAsFalseWhenProtobufFieldTypeIsTimestamp() {
        DescriptorProtos.FieldDescriptorProto fieldDescriptorProto = TestTypesMessage.getDescriptor().findFieldByName("timestamp_value").toProto();
        ProtoField protoField = new ProtoField(fieldDescriptorProto);

        assertFalse(protoField.isNested());
    }

    @Test
    public void shouldReturnNestedAsFalseWhenProtobufFieldTypeIsStruct() {
        DescriptorProtos.FieldDescriptorProto fieldDescriptorProto = TestTypesMessage.getDescriptor().findFieldByName("struct_value").toProto();
        ProtoField protoField = new ProtoField(fieldDescriptorProto);

        assertFalse(protoField.isNested());
    }

    @Test
    public void shouldReturnNestedAsFalseWhenProtobufFieldIsScalarValueTypes() {
        DescriptorProtos.FieldDescriptorProto fieldDescriptorProto = TestTypesMessage.getDescriptor().findFieldByName("timestamp_value").toProto();
        ProtoField protoField = new ProtoField(fieldDescriptorProto);

        assertFalse(protoField.isNested());
    }

    @Test
    public void shouldReturnProtoFieldString() {
        Descriptors.FieldDescriptor fieldDescriptor = TestTypesMessage.getDescriptor().findFieldByName("message_value");
        DescriptorProtos.FieldDescriptorProto fieldDescriptorProto = fieldDescriptor.toProto();
        ProtoField protoField = new ProtoField(fieldDescriptorProto);

        List<Descriptors.FieldDescriptor> childFields = fieldDescriptor.getMessageType().getFields();
        List<ProtoField> fieldList = childFields.stream().map(fd -> new ProtoField(fd.toProto())).collect(Collectors.toList());
        fieldList.forEach(pf ->
                protoField.addField(pf));

        String protoString = protoField.toString();

        assertEquals("{name='message_value', type=TYPE_MESSAGE, len=3, nested=["
                + "{name='order_number', type=TYPE_STRING, len=0, nested=[]}, "
                + "{name='order_url', type=TYPE_STRING, len=0, nested=[]}, "
                + "{name='order_details', type=TYPE_STRING, len=0, nested=[]}]}", protoString);
    }


    @Test
    public void shouldReturnEmptyProtoFieldString() {
        String protoString = new ProtoField().toString();

        assertEquals("{name='null', type=null, len=0, nested=[]}", protoString);
    }
}
