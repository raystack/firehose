package io.odpf.firehose.sink.objectstorage.writer.local;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.firehose.sink.objectstorage.Constants;
import io.odpf.firehose.sink.objectstorage.message.Record;
import io.odpf.firehose.sink.objectstorage.proto.KafkaMetadataProto;
import lombok.AllArgsConstructor;

import java.nio.file.Path;
import java.time.Instant;

/**
 * Create path partition from Record.
 */
@AllArgsConstructor
public class FilePartitionPathFactory {

    private final String kafkaMetadataFieldName;
    private final String fieldName;
    private FilePartitionPathConfig filePartitionPathConfig;

    public Path getPartitionPath(Record record) {
        FilePartitionPath filePartitionPath = getFilePartitionPath(record);
        return filePartitionPath.getPath();
    }

    public FilePartitionPath getFilePartitionPath(Record record) {
        DynamicMessage metadataMessage = record.getMetadata();
        String topic = getTopic(metadataMessage);

        Instant timestamp = null;
        if (filePartitionPathConfig.getFilePartitionType() != Constants.FilePartitionType.NONE) {
            DynamicMessage message = record.getMessage();
            timestamp = getTimestamp(message);
        }

        return new FilePartitionPath(topic, timestamp, filePartitionPathConfig);
    }

    public FilePartitionPath fromFilePartitionPath(String filePartitionPath) {
        return FilePartitionPath.parseFrom(filePartitionPath, filePartitionPathConfig);
    }

    private String getTopic(DynamicMessage dynamicMessage) {
        Descriptors.Descriptor metadataDescriptor = dynamicMessage.getDescriptorForType();

        if (!kafkaMetadataFieldName.isEmpty()) {
            DynamicMessage nestedMetadataMessage = (DynamicMessage) dynamicMessage.getField(metadataDescriptor.findFieldByName(kafkaMetadataFieldName));
            Descriptors.Descriptor nestedMetadataMessageDescriptor = nestedMetadataMessage.getDescriptorForType();
            return (String) nestedMetadataMessage.getField(nestedMetadataMessageDescriptor.findFieldByName(KafkaMetadataProto.MESSAGE_TOPIC_FIELD_NAME));
        }

        return (String) dynamicMessage.getField(metadataDescriptor.findFieldByName(KafkaMetadataProto.MESSAGE_TOPIC_FIELD_NAME));
    }

    private Instant getTimestamp(DynamicMessage dynamicMessage) {
        Descriptors.Descriptor descriptor = dynamicMessage.getDescriptorForType();
        Descriptors.FieldDescriptor timestampField = descriptor.findFieldByName(fieldName);
        DynamicMessage timestamp = (DynamicMessage) dynamicMessage.getField(timestampField);
        long seconds = (long) timestamp.getField(timestamp.getDescriptorForType().findFieldByName("seconds"));
        int nanos = (int) timestamp.getField(timestamp.getDescriptorForType().findFieldByName("nanos"));
        return Instant.ofEpochSecond(seconds, nanos);
    }
}
