package io.odpf.firehose.utils;

import io.odpf.firehose.config.DlqConfig;
import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.consumer.kafka.FirehoseKafkaConsumer;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.parser.KafkaEnvironmentVariables;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Utility methods for configuration.
 */
public class KafkaUtils {

    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String GROUP_ID = "group.id";
    private static final String ENABLE_AUTO_COMMIT = "enable.auto.commit";
    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String METADATA_MAX_AGE_MS = "metadata.max.age.ms";
    private static final String MAX_POLL_RECORDS = "max.poll.records";
    private static final String SESSION_TIMEOUT_MS = "session.timeout.ms";


    /**
     * Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
     *
     * @param config         the config
     * @param kafkaConsumer  the kafka consumer
     * @param statsdReporter the statsd reporter
     */
    public static void configureSubscription(KafkaConsumerConfig config, KafkaConsumer<byte[], byte[]> kafkaConsumer, StatsDReporter statsdReporter) {
        Instrumentation instrumentation = new Instrumentation(statsdReporter, KafkaUtils.class);
        Pattern subscriptionTopicPattern = Pattern.compile(config.getSourceKafkaTopic());
        instrumentation.logInfo("consumer subscribed using pattern: {}", subscriptionTopicPattern);
        kafkaConsumer.subscribe(subscriptionTopicPattern, new ConsumerRebalancer(new Instrumentation(statsdReporter, ConsumerRebalancer.class)));
    }

    public static Map<String, Object> getConfig(KafkaConsumerConfig config, Map<String, String> extraParameters) {
        HashMap<String, Object> consumerConfigurationMap = new HashMap<String, Object>() {{
            put(BOOTSTRAP_SERVERS, config.getSourceKafkaBrokers());
            put(GROUP_ID, config.getSourceKafkaConsumerGroupId());
            put(ENABLE_AUTO_COMMIT, config.isSourceKafkaConsumerConfigAutoCommitEnable());
            put(KEY_DESERIALIZER, ByteArrayDeserializer.class.getName());
            put(VALUE_DESERIALIZER, ByteArrayDeserializer.class.getName());
            put(METADATA_MAX_AGE_MS, config.getSourceKafkaConsumerConfigMetadataMaxAgeMs());
            put(MAX_POLL_RECORDS, config.getSourceKafkaConsumerConfigMaxPollRecords());
            put(SESSION_TIMEOUT_MS, config.getSourceKafkaConsumerConfigSessionTimeoutMs());
        }};

        return merge(consumerConfigurationMap, KafkaEnvironmentVariables.parse(extraParameters));
    }

    private static Map<String, Object> merge(HashMap<String, Object> consumerConfigurationMap, Map<String, String> extraParameters) {
        consumerConfigurationMap.putAll(extraParameters);
        return consumerConfigurationMap;
    }

    /**
     * method to create the {@link FirehoseKafkaConsumer} from the parameters supplied.
     *
     * @param config               {@see KafkaConsumerConfig}
     * @param extraKafkaParameters a map containing kafka configurations available as a key/value pair.
     * @param statsDReporter       {@see StatsDClient}
     * @return {@see EsbGenericConsumer}
     */
    public static FirehoseKafkaConsumer createConsumer(KafkaConsumerConfig config, Map<String, String> extraKafkaParameters,
                                                       StatsDReporter statsDReporter, Tracer tracer) {

        KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(KafkaUtils.getConfig(config, extraKafkaParameters));
        KafkaUtils.configureSubscription(config, kafkaConsumer, statsDReporter);
        TracingKafkaConsumer<byte[], byte[]> tracingKafkaConsumer = new TracingKafkaConsumer<>(kafkaConsumer, tracer);
        return new FirehoseKafkaConsumer(
                tracingKafkaConsumer,
                config,
                new Instrumentation(statsDReporter, FirehoseKafkaConsumer.class));
    }

    /**
     * Gets kafka producer.
     *
     * @param config the config
     * @return the kafka producer
     */
    public static KafkaProducer<byte[], byte[]> getKafkaProducer(DlqConfig config) {
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getDlqKafkaBrokers());
        props.put("acks", config.getDlqKafkaAcks());
        props.put("retries", config.getDlqKafkaRetries());
        props.put("batch.size", config.getDlqKafkaBatchSize());
        props.put("linger.ms", config.getDlqKafkaLingerMs());
        props.put("buffer.memory", config.getDlqKafkaBufferMemory());
        props.put("key.serializer", config.getDlqKafkaKeySerializer());
        props.put("value.serializer", config.getDlqKafkaValueSerializer());

        return new KafkaProducer<>(props);
    }
}
