package io.odpf.firehose.sink.file;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.StringValue;
import io.odpf.firehose.sink.file.proto.KafkaMetadataProto;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;

// TODO: 21/05/21 this should modify path builder and need to add test
public class DateHourPathFactory implements PathFactory {
    private String kafkaMetadataFieldName;

    private String fieldName;
    private String pattern;
    private String zone;

    @Override
    public Path create(Record record) {
        DynamicMessage dynamicMessage = record.getMetadata();
        Instant timestamp = getTimestamp(dynamicMessage);

        String topic = getTopic(dynamicMessage);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return Paths.get(topic, formatter.format(timestamp));
    }

    private String getTopic(DynamicMessage dynamicMessage) {
        StringValue topicField;
        if (kafkaMetadataFieldName.isEmpty()) {
            topicField = (StringValue) dynamicMessage.getField(dynamicMessage.getDescriptorForType().findFieldByName(KafkaMetadataProto.MESSAGE_TOPIC_FIELD_NAME));
        }else{
            DynamicMessage nestedMetadataMessage = (DynamicMessage) dynamicMessage.getField(dynamicMessage.getDescriptorForType().findFieldByName(kafkaMetadataFieldName));
            topicField = (StringValue) nestedMetadataMessage.getField(dynamicMessage.getDescriptorForType().findFieldByName(KafkaMetadataProto.MESSAGE_TOPIC_FIELD_NAME));
        }
        return topicField.getValue();
    }

    private Instant getTimestamp(DynamicMessage dynamicMessage) {
        DynamicMessage timestampMessage = (DynamicMessage) dynamicMessage.getField(dynamicMessage.getDescriptorForType().findFieldByName(fieldName));
        List<Descriptors.FieldDescriptor> timestampFields = timestampMessage.getDescriptorForType().getFields();

        Long epochSecond = (Long) timestampMessage.getField(timestampFields.get(0));
        int nanos = (int) timestampMessage.getField(timestampFields.get(1));

        return Instant.ofEpochSecond(epochSecond, nanos);
    }
}
