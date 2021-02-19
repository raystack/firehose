package com.gojek.esb.metrics;

import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.util.Clock;
import com.timgroup.statsd.NoOpStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StatsDReporterFactory
 * <p>
 * Create statsDReporter Instance.
 */
public class StatsDReporterFactory {

  private String statsDHost;
  private Integer statsDPort;
  private String[] globalTags;
  private static final Logger LOGGER = LoggerFactory.getLogger(StatsDReporterFactory.class);

  public StatsDReporterFactory(String statsDHost, Integer statsDPort, String[] globalTags) {
    this.statsDHost = statsDHost;
    this.statsDPort = statsDPort;
    this.globalTags = globalTags;
    LOGGER.debug("\n\tStatsd Host: {}\n\tStatsd Port: {}\n\tStatsd Tags: {}", this.statsDHost, this.statsDPort, this.globalTags);
  }

  public static StatsDReporterFactory fromKafkaConsumerConfig(KafkaConsumerConfig kafkaConsumerConfig) {
    return new StatsDReporterFactory(
      kafkaConsumerConfig.getStatsDHost(),
      kafkaConsumerConfig.getStatsDPort(),
      kafkaConsumerConfig.getStatsDTags().split(","));
  }

  public StatsDReporter buildReporter() {
    StatsDClient statsDClient = buildStatsDClient();
    Clock clockInstance = new Clock();
    return new StatsDReporter(statsDClient, clockInstance, globalTags);
  }

  private StatsDClient buildStatsDClient() {
    StatsDClient statsDClient;
    try {
      statsDClient = new NonBlockingStatsDClient("firehose", statsDHost, statsDPort);
      LOGGER.info("NonBlocking StatsD client connection established");
    } catch (Exception e) {
      LOGGER.warn("Exception on creating StatsD client, disabling StatsD and Audit client", e);
      LOGGER.warn("Firehose is running without collecting any metrics!!!!!!!!");
      statsDClient = new NoOpStatsDClient();
    }
    return statsDClient;
  }
}
