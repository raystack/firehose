package com.gojek.esb.factory;

import com.gojek.esb.config.AuditConfig;
import com.gojek.esb.config.DisabledAuditConfig;
import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.config.RetryQueueConfig;
import com.gojek.esb.consumer.EsbGenericConsumer;
import com.gojek.esb.consumer.Offsets;
import com.gojek.esb.consumer.TopicOffsets;
import com.gojek.esb.consumer.TopicPartitionOffsets;
import com.gojek.esb.filter.Filter;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.server.AuditServiceClient;
import com.gojek.esb.server.AuditServiceClientFactory;
import com.timgroup.statsd.StatsDClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * A factory class to instantiate a kafka consumer.
 */
public class GenericKafkaFactory {

    /**
     * method to create the {@link EsbGenericConsumer} from the parameters supplied.
     *
     * @param config               {@see KafkaConsumerConfig}
     * @param auditConfig          {@see AuditConfig}
     * @param extraKafkaParameters a map containing kafka configurations available as a key/value pair.
     * @param statsDReporter       {@see StatsDClient}
     * @param filter               {@see Filter}, {@see com.gojek.esb.filter.EsbMessageFilter}
     * @return {@see EsbGenericConsumer}
     */
    public EsbGenericConsumer createConsumer(KafkaConsumerConfig config, AuditConfig auditConfig, Map<String, String> extraKafkaParameters,
                                             StatsDReporter statsDReporter, Filter filter) {
        KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(FactoryUtil.getConfig(config, extraKafkaParameters));
        FactoryUtil.configureSubscription(config, kafkaConsumer);
        Optional<StatsDClient> statsDClient = statsDReporter.getClient();
        AuditServiceClient auditServiceClient;
        String auditSource = "firehose";
        auditServiceClient = statsDClient.map(c -> new AuditServiceClientFactory(auditConfig, auditSource).create(c))
                .orElseGet(() -> new AuditServiceClientFactory(new DisabledAuditConfig(), auditSource).create(null));
        Offsets offsets = !config.commitOnlyCurrentPartitions()
                ? new TopicOffsets(kafkaConsumer, config, statsDReporter)
                : new TopicPartitionOffsets(kafkaConsumer, config, statsDReporter);
        return new EsbGenericConsumer(kafkaConsumer, config, auditServiceClient, filter, offsets, statsDReporter);
    }

    public KafkaProducer<byte[], byte[]> getKafkaProducer(RetryQueueConfig config) {
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getRetryQueueKafkaBootStrapServers());
        props.put("acks", config.getRetryQueueKafkaAcks());
        props.put("retries", config.getRetryQueueKafkaRetries());
        props.put("batch.size", config.getRetryQueueKafkaBatchSize());
        props.put("linger.ms", config.getRetryQueueKafkaLingerSize());
        props.put("buffer.memory", config.getRetryQueueKafkaBufferMemory());
        props.put("key.serializer", config.getRetryQueueKafkaKeySerializer());
        props.put("value.serializer", config.getRetryQueueKafkaValueSerializer());

        return new KafkaProducer<>(props);
    }
}
