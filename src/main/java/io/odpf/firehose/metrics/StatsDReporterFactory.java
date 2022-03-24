package io.odpf.firehose.metrics;

import com.timgroup.statsd.NoOpStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.StatsDClient;
import io.odpf.firehose.config.KafkaConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StatsDReporterFactory
 * <p>
 * Create statsDReporter Instance.
 */
public class StatsDReporterFactory {

    private final String statsDHost;
    private final Integer statsDPort;
    private final String[] globalTags;
    private static final Logger LOGGER = LoggerFactory.getLogger(StatsDReporterFactory.class);

    public StatsDReporterFactory(String statsDHost, Integer statsDPort, String[] globalTags) {
        this.statsDHost = statsDHost;
        this.statsDPort = statsDPort;
        this.globalTags = globalTags;
        LOGGER.debug("\n\tStatsd Host: {}\n\tStatsd Port: {}\n\tStatsd Tags: {}", this.statsDHost, this.statsDPort, this.globalTags);
    }

    private static <T> T[] append(T[] arr, T lastElement) {
        final int length = arr.length;
        arr = java.util.Arrays.copyOf(arr, length + 1);
        arr[length] = lastElement;
        return arr;
    }

    public static StatsDReporterFactory fromKafkaConsumerConfig(KafkaConsumerConfig kafkaConsumerConfig) {
        return new StatsDReporterFactory(
                kafkaConsumerConfig.getMetricStatsDHost(),
                kafkaConsumerConfig.getMetricStatsDPort(),
                append(kafkaConsumerConfig.getMetricStatsDTags().split(","),
                        Metrics.tag(Metrics.CONSUMER_GROUP_ID_TAG, kafkaConsumerConfig.getSourceKafkaConsumerGroupId())));
    }

    public StatsDReporter buildReporter() {
        StatsDClient statsDClient = buildStatsDClient();
        return new StatsDReporter(statsDClient, globalTags);
    }

    private StatsDClient buildStatsDClient() {
        StatsDClient statsDClient;
        try {
            statsDClient = new NonBlockingStatsDClientBuilder().hostname(statsDHost).port(statsDPort).build();
            LOGGER.info("NonBlocking StatsD client connection established");
        } catch (Exception e) {
            LOGGER.warn("Exception on creating StatsD client, disabling StatsD and Audit client", e);
            LOGGER.warn("Firehose is running without collecting any metrics!!!!!!!!");
            statsDClient = new NoOpStatsDClient();
        }
        return statsDClient;
    }
}
