package io.odpf.firehose.sink.objectstorage.proto;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.Test;

import java.time.Instant;

import static io.odpf.firehose.sink.objectstorage.proto.KafkaMetadataProtoUtils.FILE_NAME;
import static io.odpf.firehose.sink.objectstorage.proto.KafkaMetadataProtoUtils.PACKAGE;
import static org.junit.Assert.*;

public class NestedKafkaMetadataMessageTest {

    private DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder().setName(FILE_NAME).setPackage(PACKAGE);
    private final String metadataColumnName = "metadata";

    @Test
    public void shouldGenerateMessageDefinition() throws Descriptors.DescriptorValidationException {
        MessageDefinition kafkaMetadataMessageDefinition = KafkaMetadataMessage.createMessageDefinition();
        schemaBuilder.addMessageDefinition(kafkaMetadataMessageDefinition);
        schemaBuilder.addMessageDefinition(TimestampMetadataMessage.createMessageDefinition());
        MessageDefinition nestedKafkaMetadataMessageDefinition = NestedKafkaMetadataMessage.createMessageDefinition(metadataColumnName,
                KafkaMetadataMessage.getTypeName(),
                kafkaMetadataMessageDefinition);
        schemaBuilder.addMessageDefinition(nestedKafkaMetadataMessageDefinition);

        DynamicSchema dynamicSchema = schemaBuilder.build();

        Descriptors.Descriptor nestedDescriptor = dynamicSchema.getMessageDescriptor(NestedKafkaMetadataMessage.getTypeName());
        assertNotNull(nestedDescriptor.findFieldByName(metadataColumnName));

        Descriptors.Descriptor metadataDescriptor = nestedDescriptor.findFieldByName(metadataColumnName).getMessageType();
        assertNotNull(metadataDescriptor.findFieldByName(KafkaMetadataMessage.MESSAGE_OFFSET_FIELD_NAME));
        assertNotNull(metadataDescriptor.findFieldByName(KafkaMetadataMessage.MESSAGE_PARTITION_FIELD_NAME));
        assertNotNull(metadataDescriptor.findFieldByName(KafkaMetadataMessage.MESSAGE_TOPIC_FIELD_NAME));
        assertNotNull(metadataDescriptor.findFieldByName(KafkaMetadataMessage.MESSAGE_TIMESTAMP_FIELD_NAME));
        assertNotNull(metadataDescriptor.findFieldByName(KafkaMetadataMessage.LOAD_TIME_FIELD_NAME));
    }

    @Test
    public void shouldCreateDynamicMessage() throws Descriptors.DescriptorValidationException {
        MessageDefinition kafkaMetadataMessageDefinition = KafkaMetadataMessage.createMessageDefinition();
        schemaBuilder.addMessageDefinition(kafkaMetadataMessageDefinition);
        schemaBuilder.addMessageDefinition(TimestampMetadataMessage.createMessageDefinition());
        MessageDefinition nestedKafkaMetadataMessageDefinition = NestedKafkaMetadataMessage.createMessageDefinition(metadataColumnName,
                KafkaMetadataMessage.getTypeName(),
                kafkaMetadataMessageDefinition);
        schemaBuilder.addMessageDefinition(nestedKafkaMetadataMessageDefinition);
        DynamicSchema dynamicSchema = schemaBuilder.build();

        Descriptors.Descriptor metadataDescriptor = dynamicSchema.getMessageDescriptor(KafkaMetadataMessage.getTypeName());
        DynamicMessage metadataMessage = KafkaMetadataMessage.newBuilder(metadataDescriptor)
                .setOffset(1)
                .setPartition(1)
                .setTopic("default")
                .setMessageTimestamp(Instant.EPOCH)
                .setLoadTime(Instant.EPOCH)
                .build();

        Descriptors.Descriptor nestedMetadataDescriptor = dynamicSchema.getMessageDescriptor(NestedKafkaMetadataMessage.getTypeName());
        DynamicMessage nestedMetadataMessage = NestedKafkaMetadataMessage.newMessageBuilder(nestedMetadataDescriptor)
                .setMetadata(metadataMessage)
                .setMetadataColumnName(metadataColumnName)
                .build();

        assertEquals(metadataMessage, nestedMetadataMessage.getField(nestedMetadataDescriptor.findFieldByName(metadataColumnName)));
    }
}
