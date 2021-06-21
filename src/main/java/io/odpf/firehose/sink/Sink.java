package io.odpf.firehose.sink;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An interface for developing custom Sinks to FirehoseConsumer.
 */
public interface Sink extends Closeable {

    /**
     * method to write batch of messages read from kafka.
     * The logic of how to persist the data goes in here.
     *
     * @param message list of {@see EsbMessage}
     * @return the list of messages
     * @throws IOException           in case of error conditions while persisting it to the custom sink.
     * @throws DeserializerException in case of problems with deserialising the message into a protobuf object.
     */
    List<Message> pushMessage(List<Message> message) throws IOException, DeserializerException;

    default Map<TopicPartition, OffsetAndMetadata> getCommittableOffset() {
        return new HashMap<>();
    }

    default boolean canSyncCommit() {
        return true;
    }
}
