package io.odpf.firehose.consumer;

import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.metrics.Instrumentation;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

import static io.odpf.firehose.metrics.Metrics.SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL;
import static io.odpf.firehose.metrics.Metrics.FAILURE_TAG;
import static io.odpf.firehose.metrics.Metrics.SUCCESS_TAG;

/**
 * Commit offsets for individual Topic partitions.
 */
@AllArgsConstructor
public class TopicPartitionOffsets implements Offsets {

    private KafkaConsumer kafkaConsumer;
    private KafkaConsumerConfig kafkaConsumerConfig;
    private Instrumentation instrumentation;

    @Override
    public void commit(ConsumerRecords<byte[], byte[]> records) {
        Map<TopicPartition, OffsetAndMetadata> offsets = createOffsetsAndMetadata(records);

        if (kafkaConsumerConfig.isSourceKafkaAsyncCommitEnable()) {
            commitAsync(offsets);
        } else {
            kafkaConsumer.commitSync(offsets);
        }
    }

    private void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        kafkaConsumer.commitAsync(offsets, (offset, exception) -> {
            if (exception != null) {
                instrumentation.incrementCounterWithTags(SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL, FAILURE_TAG);
            } else {
                instrumentation.incrementCounterWithTags(SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL, SUCCESS_TAG);
            }
        });
    }

    /**
     * Create offsets and metadata map.
     *
     * @param records the records
     * @return the map
     */
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
