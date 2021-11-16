package io.odpf.firehose.sink.blob;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.sink.blob.message.KafkaMetadataUtils;
import io.odpf.firehose.sink.blob.proto.KafkaMetadataProtoMessageUtils;

import java.time.Instant;

public class TestUtils {

    public static DynamicMessage createMetadata(String kafkaMetadataColumnName, Instant eventTimestamp, long offset, int partition, String topic) {
        Message message = new Message("".getBytes(), "".getBytes(), topic, partition, offset, null, eventTimestamp.toEpochMilli(), eventTimestamp.toEpochMilli());
        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoMessageUtils.createFileDescriptor(kafkaMetadataColumnName);
        return KafkaMetadataUtils.createKafkaMetadata(fileDescriptor, message, kafkaMetadataColumnName);
    }

    public static DynamicMessage createMessage(Instant timestamp, int orderNum) {
        TestProtoMessage.MessageBuilder messageBuilder = TestProtoMessage.createMessageBuilder();
        return messageBuilder
                .setCreatedTime(timestamp)
                .setOrderNumber(orderNum).build();
    }
}
