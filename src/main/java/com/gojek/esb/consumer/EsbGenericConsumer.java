package com.gojek.esb.consumer;

import com.gojek.esb.audit.AuditableProtoMessage;
import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.filter.EsbFilterException;
import com.gojek.esb.filter.Filter;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.server.AuditServiceClient;
import com.newrelic.api.agent.Trace;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.gojek.esb.metrics.Metrics.KAFKA_FILTERED_MESSAGE;

/**
 * A class responsible for consuming the messages in kafka.
 * It is capable of applying filters supplied while instantiating this consumer {@see com.gojek.esb.factory.GenericKafkaFactory},
 * {@see Filter}. Audit client helps audit the messages.
 */
public class EsbGenericConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(EsbGenericConsumer.class.getName());

    private final KafkaConsumer kafkaConsumer;
    private final KafkaConsumerConfig consumerConfig;
    private final AuditServiceClient auditServiceClient;
    private Filter filter;
    private Offsets offsets;
    private StatsDReporter statsDReporter;

    private ConsumerRecords<byte[], byte[]> records;

    /**
     * A Constructor.
     *
     * @param kafkaConsumer      {@see KafkaConsumer}
     * @param config             Consumer configuration.
     * @param auditServiceClient {@see AuditServiceClient}
     * @param filter             a Filter implementation to filter the messages. {@see Filter}, {@see com.gojek.esb.filter.EsbMessageFilter}
     * @param offsets            {@see Offsets}
     * @param statsDReporter     Sends StatsD metric
     */
    public EsbGenericConsumer(KafkaConsumer kafkaConsumer, KafkaConsumerConfig config, AuditServiceClient auditServiceClient, Filter filter,
                              Offsets offsets, StatsDReporter statsDReporter) {
        this.kafkaConsumer = kafkaConsumer;
        this.consumerConfig = config;
        this.auditServiceClient = auditServiceClient;
        this.filter = filter;
        this.offsets = offsets;
        this.statsDReporter = statsDReporter;
    }

    /**
     * method to read next batch of messages from kafka.
     *
     * @return list of EsbMessage {@see EsbMessage}
     * @throws EsbFilterException in case of error when applying the filter condition.
     */
    public List<EsbMessage> readMessages() throws EsbFilterException {
        this.records = kafkaConsumer.poll(consumerConfig.getPollTimeOut());
        LOGGER.info("Pulled {} messages", records.count());
        List<EsbMessage> messages = new ArrayList<>();

        for (ConsumerRecord<byte[], byte[]> record : records) {
            LOGGER.debug("Pulled record: {}", record);
            messages.add(new EsbMessage(record.key(), record.value(), record.topic(), record.partition(), record.offset()));
        }

        audit(messages);

        return filter(messages);
    }

    private void audit(List<EsbMessage> messages) {
        if (messages.isEmpty()) {
            return;
        }
        Stream<EsbMessage> messagesWithKey = messages
                .stream()
                .filter(esbMessage -> esbMessage.getLogKey() != null);
        auditServiceClient.sendAuditableMessages(
                messagesWithKey.map(esbMessage -> new AuditableProtoMessage(esbMessage.getLogKey(), esbMessage.getTopic()))
        );
    }

    private List<EsbMessage> filter(List<EsbMessage> messages) throws EsbFilterException {
        List<EsbMessage> filteredMessage = filter.filter(messages);
        Integer filteredMessageCount = messages.size() - filteredMessage.size();
        if (filteredMessageCount > 0) {
            statsDReporter.captureCount(KAFKA_FILTERED_MESSAGE, filteredMessageCount, "expr=" + consumerConfig.getFilterExpression());
        }
        return filteredMessage;
    }

    @Trace(dispatcher = true)
    public void commit() {
        offsets.commit(records);
    }

    public void close() {
        try {
            this.kafkaConsumer.close();
        } catch (Exception e) {
            LOGGER.error("Exception while closing ", e);
        }
    }
}
