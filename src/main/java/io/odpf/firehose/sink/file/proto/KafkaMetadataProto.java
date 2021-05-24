package io.odpf.firehose.sink.file.proto;

import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;

import java.time.Instant;

public class KafkaMetadataProto {
    private static final String TYPE_NAME = "KafkaOffsetMetadata";

    public static final String MESSAGE_OFFSET_FIELD_NAME = "message_offset";
    public static final String MESSAGE_PARTITION_FIELD_NAME = "message_partition";
    public static final String MESSAGE_TOPIC_FIELD_NAME = "message_topic";
    public static final String MESSAGE_TIMESTAMP_FIELD_NAME = "message_timestamp";
    public static final String LOAD_TIME_FIELD_NAME = "load_time";

    public static MessageDefinition createMessageDefinition() {
        return MessageDefinition.newBuilder(TYPE_NAME)
                .addField("optional", "int64", MESSAGE_OFFSET_FIELD_NAME, 536870907)
                .addField("optional", "int32", MESSAGE_PARTITION_FIELD_NAME, 536870908)
                .addField("optional", "string", MESSAGE_TOPIC_FIELD_NAME, 536870909)
                .addField("optional", "Timestamp", MESSAGE_TIMESTAMP_FIELD_NAME, 536870910)
                .addField("optional", "Timestamp", LOAD_TIME_FIELD_NAME, 536870911)
                .build();
    }

    public static String getTypeName() {
        return TYPE_NAME;
    }


    public static class MessageBuilder {

        private String topic;
        private int partition;
        private long offset;
        private Instant loadTime;
        private Instant messageTimestamp;

        private Descriptors.Descriptor descriptor;

        public MessageBuilder(Descriptors.Descriptor descriptor) {
            this.descriptor = descriptor;
        }


        public MessageBuilder setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public MessageBuilder setPartition(int partition) {
            this.partition = partition;
            return this;
        }

        public MessageBuilder setOffset(long offset) {
            this.offset = offset;
            return this;
        }

        public MessageBuilder setLoadTime(Instant loadTime) {
            this.loadTime = loadTime;
            return this;
        }

        public MessageBuilder setMessageTimestamp(Instant messageTimestamp) {
            this.messageTimestamp = messageTimestamp;
            return this;
        }

        public DynamicMessage build(){
            return DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName(LOAD_TIME_FIELD_NAME), TimestampProto.newBuilder()
                            .setSeconds(loadTime.getEpochSecond())
                            .setNanos(loadTime.getNano())
                            .build())
                    .setField(descriptor.findFieldByName(MESSAGE_TIMESTAMP_FIELD_NAME), TimestampProto.newBuilder()
                            .setSeconds(messageTimestamp.getEpochSecond())
                            .setNanos(messageTimestamp.getNano())
                            .build())
                    .setField(descriptor.findFieldByName(MESSAGE_OFFSET_FIELD_NAME), offset)
                    .setField(descriptor.findFieldByName(MESSAGE_PARTITION_FIELD_NAME), partition)
                    .setField(descriptor.findFieldByName(MESSAGE_TOPIC_FIELD_NAME), topic)
                    .build();
        }
    }

    public static MessageBuilder newBuilder(Descriptors.Descriptor descriptor){
        return new MessageBuilder(descriptor);
    }
}
