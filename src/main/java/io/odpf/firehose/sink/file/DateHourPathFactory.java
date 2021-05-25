package io.odpf.firehose.sink.file;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.odpf.firehose.sink.file.proto.KafkaMetadataProto;
import lombok.AllArgsConstructor;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

@AllArgsConstructor
public class DateHourPathFactory implements PathFactory {

    private String kafkaMetadataFieldName;
    private String fieldName;
    private String dateTimePattern;
    private String zone;
    private String prefix;

    @Override
    public Path create(Record record) {
        DynamicMessage message = record.getMessage();
        Instant timestamp = getTimestamp(message);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(timestamp, ZoneId.of(zone));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateTimePattern);
        String dateTime = formatter.format(localDateTime);
        String dateSegment = String.format("%s%s", prefix, dateTime);

        DynamicMessage metadataMessage = record.getMetadata();
        String topic = getTopic(metadataMessage);

        return Paths.get(topic, dateSegment);
    }

    private String getTopic(DynamicMessage dynamicMessage) {
        Descriptors.Descriptor metadataDescriptor = dynamicMessage.getDescriptorForType();;

        if (!kafkaMetadataFieldName.isEmpty()) {
            DynamicMessage nestedMetadataMessage = (DynamicMessage) dynamicMessage.getField(metadataDescriptor.findFieldByName(kafkaMetadataFieldName));
            Descriptors.Descriptor nestedMetadataMessageDescriptor= nestedMetadataMessage.getDescriptorForType();
            return (String) nestedMetadataMessage.getField(nestedMetadataMessageDescriptor.findFieldByName(KafkaMetadataProto.MESSAGE_TOPIC_FIELD_NAME));
        }

        return (String) dynamicMessage.getField(metadataDescriptor.findFieldByName(KafkaMetadataProto.MESSAGE_TOPIC_FIELD_NAME));
    }

    private Instant getTimestamp(DynamicMessage dynamicMessage) {
        Descriptors.Descriptor descriptor = dynamicMessage.getDescriptorForType();
        Descriptors.FieldDescriptor timestampField = descriptor.findFieldByName(fieldName);
        Timestamp timestamp = (Timestamp) dynamicMessage.getField(timestampField);
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}
