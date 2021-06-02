package io.odpf.firehose.sink.cloud.writer.path;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.firehose.exception.EglcConfigurationException;
import io.odpf.firehose.sink.cloud.message.Record;
import io.odpf.firehose.sink.cloud.proto.KafkaMetadataProto;
import io.odpf.firehose.sink.cloud.writer.PartitioningType;
import lombok.AllArgsConstructor;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@AllArgsConstructor
public class TimePartitionPath {

    private String kafkaMetadataFieldName;
    private String fieldName;
    private String datePattern;
    private PartitioningType partitioningType;
    private String zone;
    private String datePrefix;
    private String hourPrefix;

    public Path create(Record record) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(datePattern);

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
