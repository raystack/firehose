package com.gojek.esb.factory;

import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.consumer.ConsumerRebalancer;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.parser.KafkaEnvironmentVariables;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class FactoryUtil {

    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String GROUP_ID = "group.id";
    private static final String ENABLE_AUTO_COMMIT = "enable.auto.commit";
    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String METADATA_MAX_AGE_MS = "metadata.max.age.ms";
    private static final String MAX_POLL_RECORDS = "max.poll.records";
    private static final String SESSION_TIMEOUT_MS = "session.timeout.ms";


    public static void configureSubscription(KafkaConsumerConfig config, KafkaConsumer kafkaConsumer, StatsDReporter statsdReporter) {
        Instrumentation instrumentation = new Instrumentation(statsdReporter, FactoryUtil.class);
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
}
