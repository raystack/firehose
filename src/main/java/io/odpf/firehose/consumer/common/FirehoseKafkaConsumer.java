package io.odpf.firehose.consumer.common;

import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.Metrics;
import io.odpf.firehose.message.Message;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.odpf.firehose.metrics.Metrics.FAILURE_TAG;
import static io.odpf.firehose.metrics.Metrics.SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL;
import static io.odpf.firehose.metrics.Metrics.SUCCESS_TAG;

/**
 * A class responsible for consuming and committing kafka records.
 */
public class FirehoseKafkaConsumer implements AutoCloseable {

    private final Consumer<byte[], byte[]> kafkaConsumer;
    private final KafkaConsumerConfig consumerConfig;
    private final Instrumentation instrumentation;
    private final Map<TopicPartition, OffsetAndMetadata> committedOffsets = new ConcurrentHashMap<>();

    /**
     * A Constructor.
     *
     * @param kafkaConsumer   {@see KafkaConsumer}
     * @param config          Consumer configuration.
     * @param instrumentation Contain logging and metrics collection
     */
    public FirehoseKafkaConsumer(Consumer<byte[], byte[]> kafkaConsumer, KafkaConsumerConfig config, Instrumentation instrumentation) {
        this.kafkaConsumer = kafkaConsumer;
        this.consumerConfig = config;
        this.instrumentation = instrumentation;
    }

    /**
     * method to read next batch of messages from kafka.
     *
     * @return list of EsbMessage {@see EsbMessage}
     */
    public List<Message> readMessages() {
        ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(consumerConfig.getSourceKafkaPollTimeoutMs()));
        instrumentation.logInfo("Pulled {} messages", records.count());
        instrumentation.capturePulledMessageHistogram(records.count());
        instrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.CONSUMER, records.count());
        List<Message> messages = new ArrayList<>();

        for (ConsumerRecord<byte[], byte[]> record : records) {
            messages.add(new Message(record.key(), record.value(), record.topic(), record.partition(), record.offset(), record.headers(), record.timestamp(), System.currentTimeMillis()));
            instrumentation.logDebug("Pulled record: {}", record);
        }
        return messages;
    }

    public void close() {
        try {
            instrumentation.logInfo("Consumer is closing");
            this.kafkaConsumer.close();
        } catch (Exception e) {
            instrumentation.captureNonFatalError(e, "Exception while closing consumer");
        }
    }

    public void commit() {
        if (consumerConfig.isSourceKafkaAsyncCommitEnable()) {
            kafkaConsumer.commitAsync((offsets, exception) -> {
                if (exception != null) {
                    instrumentation.incrementCounter(SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL, FAILURE_TAG);
                } else {
                    instrumentation.incrementCounter(SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL, SUCCESS_TAG);
                }
            });
        } else {
            kafkaConsumer.commitSync();
        }
    }

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
        latestOffsets.forEach((k, v) ->
                instrumentation.logInfo("Committing Offsets " + k.topic() + ":" + k.partition() + "=>" + v.offset()));
        if (consumerConfig.isSourceKafkaAsyncCommitEnable()) {
            commitAsync(latestOffsets);
        } else {
            kafkaConsumer.commitSync(latestOffsets);
            committedOffsets.putAll(offsets);
        }
    }

    private void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        kafkaConsumer.commitAsync(offsets, this::onComplete);
    }

    private void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (exception != null) {
            instrumentation.incrementCounter(SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL, FAILURE_TAG);
        } else {
            instrumentation.incrementCounter(SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL, SUCCESS_TAG);
            committedOffsets.putAll(offsets);
        }
    }
}
