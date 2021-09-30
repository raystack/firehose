package io.odpf.firehose.sink.bigquery.converter.fields;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.odpf.firehose.consumer.TestBytesMessage;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.Assert.*;


public class ByteProtoFieldTest {

    private ByteProtoField byteProtoField;
    private final String content = "downing street";

    @Before
    public void setUp() throws Exception {
        TestBytesMessage bytesMessage = TestBytesMessage.newBuilder()
                .setContent(ByteString.copyFromUtf8(content))
                .build();

        Descriptors.FieldDescriptor fieldDescriptor = bytesMessage.getDescriptorForType().findFieldByName("content");
        byteProtoField = new ByteProtoField(fieldDescriptor, bytesMessage.getField(fieldDescriptor));
    }

    @Test
    public void shouldConvertBytesToString() {
        String parseResult = (String) byteProtoField.getValue();
        String encodedBytes = new String(Base64.getEncoder().encode(content.getBytes(StandardCharsets.UTF_8)));
        assertEquals(encodedBytes, parseResult);
    }

    @Test
    public void shouldMatchByteProtobufField() {
        assertTrue(byteProtoField.matches());
    }

    @Test
    public void shouldNotMatchFieldOtherThanByteProtobufField() {
        TestBytesMessage bytesMessage = TestBytesMessage.newBuilder()
                .build();
        Descriptors.FieldDescriptor fieldDescriptor = bytesMessage.getDescriptorForType().findFieldByName("order_number");
        byteProtoField = new ByteProtoField(fieldDescriptor, bytesMessage.getField(fieldDescriptor));

        assertFalse(byteProtoField.matches());
    }
}
