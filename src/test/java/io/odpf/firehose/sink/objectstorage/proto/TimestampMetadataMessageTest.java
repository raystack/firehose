package io.odpf.firehose.sink.objectstorage.proto;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import org.junit.Test;

import static io.odpf.firehose.sink.objectstorage.proto.KafkaMetadataProtoUtils.FILE_NAME;
import static io.odpf.firehose.sink.objectstorage.proto.KafkaMetadataProtoUtils.PACKAGE;
import static org.junit.Assert.*;

public class TimestampMetadataMessageTest {

    private DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder().setName(FILE_NAME).setPackage(PACKAGE);

    @Test
    public void shouldGenerateMessageDefinition() throws Descriptors.DescriptorValidationException {
        schemaBuilder.addMessageDefinition(TimestampMetadataMessage.createMessageDefinition());
        DynamicSchema dynamicSchema = schemaBuilder.build();

        Descriptors.Descriptor descriptor = dynamicSchema.getMessageDescriptor(TimestampMetadataMessage.getTypeName());
        assertNotNull(descriptor.findFieldByName(TimestampMetadataMessage.NANOS_FIELD_NAME));
        assertNotNull(descriptor.findFieldByName(TimestampMetadataMessage.SECONDS_FIELD_NAME));
    }

    @Test
    public void shouldCreateDynamicMessage() {
        Timestamp timestamp = TimestampMetadataMessage.newBuilder().setNanos(1).setSeconds(1)
                .build();
        assertEquals(1, timestamp.getNanos());
        assertEquals(1, timestamp.getSeconds());
    }
}
