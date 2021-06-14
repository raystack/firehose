package io.odpf.firehose.consumer.committer;

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
import java.util.stream.Collectors;

import static io.odpf.firehose.metrics.Metrics.FAILURE_TAG;
import static io.odpf.firehose.metrics.Metrics.SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL;
import static io.odpf.firehose.metrics.Metrics.SUCCESS_TAG;

/**
 * Commit offsets for individual Topic partitions.
 */
@AllArgsConstructor
public class ManagedOffsetCommitter implements OffsetCommitter {

    private final Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
    private final KafkaConsumer kafkaConsumer;
    private final KafkaConsumerConfig kafkaConsumerConfig;
    private final Instrumentation instrumentation;

    @Override
    public void commit(ConsumerRecords<byte[], byte[]> records) {
        Map<TopicPartition, OffsetAndMetadata> offsets = createOffsetsAndMetadata(records);
        commit(offsets);
    }

    @Override
    public void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        Map<TopicPartition, OffsetAndMetadata> latestOffsets =
                offsets.entrySet()
                        .stream()
                        .filter(metadataEntry -> !committedOffsets.containsKey(metadataEntry.getKey())
                                                 || metadataEntry.getValue().offset() > committedOffsets.get(metadataEntry.getKey()).offset())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (latestOffsets.isEmpty()) {
            return;
        }
        committedOffsets.putAll(latestOffsets);
        if (kafkaConsumerConfig.isSourceKafkaAsyncCommitEnable()) {
            commitAsync(latestOffsets);
        } else {
            kafkaConsumer.commitSync(latestOffsets);
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
        return (!topicPartitionAndOffset.containsKey(topicPartition))
               || (topicPartitionAndOffset.containsKey(topicPartition)
                   && topicPartitionAndOffset.get(topicPartition).offset() < offsetAndMetadata.offset());
    }
}
