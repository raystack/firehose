package com.gojek.esb.metrics;

import com.gojek.esb.util.Clock;
import com.timgroup.statsd.NoOpStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StatsDReporterFactory
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
  }

  public StatsDReporter buildReporter() {
    StatsDClient statsDClient = buildStatsDClient();
    Clock clockInstance = new Clock();
    return new StatsDReporter(statsDClient, clockInstance, globalTags);
  }

  public StatsDClient getStatsDClient() {
    return buildStatsDClient();
  }

  private StatsDClient buildStatsDClient() {
    StatsDClient statsDClient;
    try {
      statsDClient = new NonBlockingStatsDClient("firehose", statsDHost, statsDPort);
    } catch (Exception e) {
      LOGGER.error("Exception on creating StatsD client, disabling StatsD and Audit client", e);
      LOGGER.error("FireHose is running without collecting any metrics!!!!!!!!");
      statsDClient = new NoOpStatsDClient();
    }
    return statsDClient;
  }
}
