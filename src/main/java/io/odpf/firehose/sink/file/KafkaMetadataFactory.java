package io.odpf.firehose.sink.file;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.sink.file.proto.KafkaMetadataProto;
import io.odpf.firehose.sink.file.proto.KafkaMetadataProtoFile;
import io.odpf.firehose.sink.file.proto.NestedKafkaMetadataProto;

import java.time.Instant;

public class KafkaMetadataFactory implements MetadataFactory {

    private Descriptors.FileDescriptor kafkaMetadataFileDescriptor;
    private String kafkaMetadataColumnName;

    public KafkaMetadataFactory(String kafkaMetadataColumnName) {
        this.kafkaMetadataColumnName = kafkaMetadataColumnName;
        kafkaMetadataFileDescriptor = KafkaMetadataProtoFile.createFileDescriptor(kafkaMetadataColumnName);
    }

    @Override
    public DynamicMessage create(Message message){
        Descriptors.Descriptor metadataDescriptor = kafkaMetadataFileDescriptor.findMessageTypeByName(KafkaMetadataProto.getTypeName());

        Instant loadTime = Instant.now();
        Instant messageTimestamp = Instant.ofEpochMilli(message.getTimestamp());

        KafkaMetadataProto.MessageBuilder messageBuilder = KafkaMetadataProto.newBuilder(metadataDescriptor)
                .setLoadTime(loadTime)
                .setMessageTimestamp(messageTimestamp)
                .setOffset(message.getOffset())
                .setPartition(message.getPartition())
                .setTopic(message.getTopic());

        DynamicMessage metadata = messageBuilder.build();

        if (kafkaMetadataColumnName.isEmpty()){
            return metadata;
        }

        Descriptors.Descriptor nestedMetadataDescriptor = kafkaMetadataFileDescriptor.findMessageTypeByName(NestedKafkaMetadataProto.getTypeName());

        NestedKafkaMetadataProto.MessageBuilder builder = NestedKafkaMetadataProto.newMessageBuilder(nestedMetadataDescriptor)
                .setMetadata(metadata)
                .setMetadataColumnName(kafkaMetadataColumnName);

        return builder.build();
    }
}
