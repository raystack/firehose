package com.gojek.esb.consumer;

import com.gojek.esb.audit.AuditableProtoMessage;
import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.filter.EsbFilterException;
import com.gojek.esb.filter.Filter;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.server.AuditServiceClient;
import com.newrelic.api.agent.Trace;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * A class responsible for consuming the messages in kafka.
 * It is capable of applying filters supplied while instantiating this consumer {@see com.gojek.esb.factory.GenericKafkaFactory},
 * {@see Filter}. Audit client helps audit the messages.
 */
public class EsbGenericConsumer {

    private final Consumer kafkaConsumer;
    private final KafkaConsumerConfig consumerConfig;
    private final AuditServiceClient auditServiceClient;
    private Filter filter;
    private Offsets offsets;
    private Instrumentation instrumentation;

    private ConsumerRecords<byte[], byte[]> records;

    /**
     * A Constructor.
     *
     * @param kafkaConsumer      {@see KafkaConsumer}
     * @param config             Consumer configuration.
     * @param auditServiceClient {@see AuditServiceClient}
     * @param filter             a Filter implementation to filter the messages. {@see Filter}, {@see com.gojek.esb.filter.EsbMessageFilter}
     * @param offsets            {@see Offsets}
     * @param instrumentation     Contain logging and metrics collection
     */
    public EsbGenericConsumer(Consumer kafkaConsumer, KafkaConsumerConfig config, AuditServiceClient auditServiceClient, Filter filter,
                              Offsets offsets, Instrumentation instrumentation) {
        this.kafkaConsumer = kafkaConsumer;
        this.consumerConfig = config;
        this.auditServiceClient = auditServiceClient;
        this.filter = filter;
        this.offsets = offsets;
        this.instrumentation = instrumentation;
    }

    /**
     * method to read next batch of messages from kafka.
     *
     * @return list of EsbMessage {@see EsbMessage}
     * @throws EsbFilterException in case of error when applying the filter condition.
     */
    public List<EsbMessage> readMessages() throws EsbFilterException {
        this.records = kafkaConsumer.poll(consumerConfig.getPollTimeOut());
        instrumentation.logInfo("Pulled {} messages", records.count());
        List<EsbMessage> messages = new ArrayList<>();

        for (ConsumerRecord<byte[], byte[]> record : records) {
            messages.add(new EsbMessage(record.key(), record.value(), record.topic(), record.partition(), record.offset(), record.headers(), record.timestamp()));
            instrumentation.logDebug("Pulled record: {}", record);
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
            instrumentation.captureFilteredMessageCount(filteredMessageCount, consumerConfig.getFilterExpression());
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
            instrumentation.captureNonFatalError(e, "Exception while closing ");
        }
    }
}
