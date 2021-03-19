package io.odpf.firehose.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface Offsets {

    void commit(ConsumerRecords<byte[], byte[]> records);
}
