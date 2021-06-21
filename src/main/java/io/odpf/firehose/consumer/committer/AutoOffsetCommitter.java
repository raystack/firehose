package io.odpf.firehose.consumer.committer;

import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.metrics.Instrumentation;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import static io.odpf.firehose.metrics.Metrics.SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL;
import static io.odpf.firehose.metrics.Metrics.FAILURE_TAG;
import static io.odpf.firehose.metrics.Metrics.SUCCESS_TAG;

/**
 * Commits offsets for consumer records.
 */
@AllArgsConstructor
public class AutoOffsetCommitter implements OffsetCommitter {

    private KafkaConsumer kafkaConsumer;
    private KafkaConsumerConfig kafkaConsumerConfig;
    private Instrumentation instrumentation;

    @Override
    public void commit(ConsumerRecords<byte[], byte[]> records) {
        if (kafkaConsumerConfig.isSourceKafkaAsyncCommitEnable()) {
            commitAsync();
        } else {
            kafkaConsumer.commitSync();
        }
    }

    private void commitAsync() {
        kafkaConsumer.commitAsync((offsets, exception) -> {
            if (exception != null) {
                instrumentation.incrementCounterWithTags(SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL, FAILURE_TAG);
            } else {
                instrumentation.incrementCounterWithTags(SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL, SUCCESS_TAG);
            }
        });
    }
}
