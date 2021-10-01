package io.odpf.firehose.consumer;

import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.filter.Filter;
import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.metrics.Instrumentation;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * A class responsible for consuming the messages in kafka.
 * It is capable of applying filters supplied while instantiating this consumer {@see io.odpf.firehose.factory.GenericKafkaFactory},
 * {@see Filter}.
 */
public class GenericConsumer {

    private final Consumer kafkaConsumer;
    private final KafkaConsumerConfig consumerConfig;
    private Filter filter;
    private Offsets offsets;
    private Instrumentation instrumentation;

    private ConsumerRecords<byte[], byte[]> records;

    /**
     * A Constructor.
     *
     * @param kafkaConsumer   {@see KafkaConsumer}
     * @param config          Consumer configuration.
     * @param filter          a Filter implementation to filter the messages. {@see Filter}, {@see io.odpf.firehose.filter.EsbMessageFilter}
     * @param offsets         {@see Offsets}
     * @param instrumentation Contain logging and metrics collection
     */
    public GenericConsumer(Consumer kafkaConsumer, KafkaConsumerConfig config, Filter filter,
                           Offsets offsets, Instrumentation instrumentation) {
        this.kafkaConsumer = kafkaConsumer;
        this.consumerConfig = config;
        this.filter = filter;
        this.offsets = offsets;
        this.instrumentation = instrumentation;
    }

    /**
     * method to read next batch of messages from kafka.
     *
     * @return list of EsbMessage {@see EsbMessage}
     * @throws FilterException in case of error when applying the filter condition.
     */
    public List<Message> readMessages() throws FilterException {
        this.records = kafkaConsumer.poll(Duration.ofMillis(consumerConfig.getSourceKafkaPollTimeoutMs()));
        instrumentation.logInfo("Pulled {} messages", records.count());
        instrumentation.capturePulledMessageHistogram(records.count());
        List<Message> messages = new ArrayList<>();

        for (ConsumerRecord<byte[], byte[]> record : records) {
            messages.add(new Message(record.key(), record.value(), record.topic(), record.partition(), record.offset(), record.headers(), record.timestamp(), System.currentTimeMillis()));
            instrumentation.logDebug("Pulled record: {}", record);
        }
        return filter(messages);
    }

    private List<Message> filter(List<Message> messages) throws FilterException {
        List<Message> filteredMessage = filter.filter(messages);
        Integer filteredMessageCount = messages.size() - filteredMessage.size();
        if (filteredMessageCount > 0) {
            instrumentation.captureFilteredMessageCount(filteredMessageCount, filter.getFilterRule());
        }
        return filteredMessage;
    }

    public void commit() {
        offsets.commit(records);
    }

    public void close() {
        try {
            instrumentation.logInfo("Consumer is closing");
            this.kafkaConsumer.close();
        } catch (Exception e) {
            instrumentation.captureNonFatalError(e, "Exception while closing consumer");
        }
    }
}
