package com.gojek.esb.consumer;

import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.metrics.StatsDReporter;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

import static com.gojek.esb.metrics.Metrics.KAFKA_COMMIT_COUNT;
import static com.gojek.esb.metrics.Metrics.FAILURE_TAG;
import static com.gojek.esb.metrics.Metrics.SUCCESS_TAG;

@AllArgsConstructor
public class TopicPartitionOffsets implements Offsets {

    private KafkaConsumer kafkaConsumer;
    private KafkaConsumerConfig kafkaConsumerConfig;
    private StatsDReporter statsDReporter;

    @Override
    public void commit(ConsumerRecords<byte[], byte[]> records) {
        Map<TopicPartition, OffsetAndMetadata> offsets = createOffsetsAndMetadata(records);

        if (kafkaConsumerConfig.asyncCommitEnabled()) {
            commitAsync(offsets);
        } else {
            kafkaConsumer.commitSync(offsets);
        }
    }

    private void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        kafkaConsumer.commitAsync(offsets, (offset, exception) -> {
            if (exception != null) {
                statsDReporter.increment(KAFKA_COMMIT_COUNT, FAILURE_TAG);
            } else {
                statsDReporter.increment(KAFKA_COMMIT_COUNT, SUCCESS_TAG);
            }
        });
    }

    public Map<TopicPartition, OffsetAndMetadata> createOffsetsAndMetadata(ConsumerRecords<byte[], byte[]> records) {
        Map<TopicPartition, OffsetAndMetadata> topicPartitionAndOffset = new HashMap<>();

        for (ConsumerRecord<byte[], byte[]> record : records) {
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset() + 1);

            if (checkFeasibleTopicPartitionOffsetMapUpdation(topicPartitionAndOffset, topicPartition, offsetAndMetadata)) {
                topicPartitionAndOffset.put(topicPartition, offsetAndMetadata);
            }
        }

        return topicPartitionAndOffset;
    }

    private boolean checkFeasibleTopicPartitionOffsetMapUpdation(Map<TopicPartition, OffsetAndMetadata> topicPartitionAndOffset,
                                                                 TopicPartition topicPartition,
                                                                 OffsetAndMetadata offsetAndMetadata) {
        if ((!topicPartitionAndOffset.containsKey(topicPartition))
                || (topicPartitionAndOffset.containsKey(topicPartition)
                && topicPartitionAndOffset.get(topicPartition).offset() < offsetAndMetadata.offset())) {
            return true;
        }

        return false;
    }
}
