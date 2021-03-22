package io.odpf.firehose.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Commits offsets to kafka.
 */
public interface Offsets {

    void commit(ConsumerRecords<byte[], byte[]> records);
}
