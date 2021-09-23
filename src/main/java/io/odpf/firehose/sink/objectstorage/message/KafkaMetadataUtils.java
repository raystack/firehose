package io.odpf.firehose.sink.objectstorage.message;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.sink.objectstorage.proto.KafkaMetadataMessage;
import io.odpf.firehose.sink.objectstorage.proto.NestedKafkaMetadataMessage;

import java.time.Instant;

/**
 *  KafkaMetadataUtils utility class for creating kafka metadata {@link com.google.protobuf.DynamicMessage DynamicMessage}.
 */
public class KafkaMetadataUtils {

    public static DynamicMessage createKafkaMetadata(Descriptors.FileDescriptor kafkaMetadataFileDescriptor, Message message, String kafkaMetadataColumnName) {
        Descriptors.Descriptor metadataDescriptor = kafkaMetadataFileDescriptor.findMessageTypeByName(KafkaMetadataMessage.getTypeName());

        Instant loadTime = Instant.now();
        Instant messageTimestamp = Instant.ofEpochMilli(message.getTimestamp());

        KafkaMetadataMessage.MessageBuilder messageBuilder = KafkaMetadataMessage.newBuilder(metadataDescriptor)
                .setLoadTime(loadTime)
                .setMessageTimestamp(messageTimestamp)
                .setOffset(message.getOffset())
                .setPartition(message.getPartition())
                .setTopic(message.getTopic());

        DynamicMessage metadata = messageBuilder.build();

        if (kafkaMetadataColumnName.isEmpty()) {
            return metadata;
        }

        Descriptors.Descriptor nestedMetadataDescriptor = kafkaMetadataFileDescriptor.findMessageTypeByName(NestedKafkaMetadataMessage.getTypeName());

        return NestedKafkaMetadataMessage.newMessageBuilder(nestedMetadataDescriptor)
                .setMetadata(metadata)
                .setMetadataColumnName(kafkaMetadataColumnName).build();
    }
}
