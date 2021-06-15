package io.odpf.firehose.sink.objectstorage.writer.local;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.firehose.exception.EglcConfigurationException;
import io.odpf.firehose.sink.objectstorage.Constants;
import io.odpf.firehose.sink.objectstorage.message.Record;
import io.odpf.firehose.sink.objectstorage.proto.KafkaMetadataProto;
import lombok.AllArgsConstructor;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@AllArgsConstructor
public class TimePartitionPath {

    private final String kafkaMetadataFieldName;
    private final String fieldName;
    private final Constants.PartitioningType partitioningType;
    private final String zone;
    private final String datePrefix;
    private final String hourPrefix;

    public Path create(Record record) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        DynamicMessage metadataMessage = record.getMetadata();
        String topic = getTopic(metadataMessage);

        DynamicMessage message = record.getMessage();
        Instant timestamp = getTimestamp(message);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(timestamp, ZoneId.of(zone));

        String dateTime = formatter.format(localDateTime);
        String dateSegment = String.format("%s%s", datePrefix, dateTime);

        switch (partitioningType) {
            case DAY:
                return Paths.get(topic, dateSegment);
            case HOUR:
                String hour = DateTimeFormatter.ofPattern("HH").format(localDateTime);
                String hourSegment = String.format("%s%s", hourPrefix, hour);
                return Paths.get(topic, dateSegment, hourSegment);
            case NONE:
                return Paths.get(topic);
            default:
                throw new EglcConfigurationException(String.format("%s partition type is not supported", partitioningType));
        }
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
