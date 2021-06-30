package io.odpf.firehose.sink;

import io.odpf.firehose.consumer.Message;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public interface CommitOffsetManager {

    /**
     *
     * @param batch
     * @param messageList
     */
    default void addOffsetToBatch(Object batch, List<Message> messageList) {
    }

    /**
     *
     * @param batch
     */
    default void setCommittable(Object batch) {
    }

    default Map<TopicPartition, OffsetAndMetadata> getCommittableOffsets() {
        return new HashMap<>();
    }

}
