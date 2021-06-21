package io.odpf.firehose.consumer.committer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * Commits offsets to kafka.
 */
public interface OffsetCommitter {

    void commit(ConsumerRecords<byte[], byte[]> records);

    default void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }
}
