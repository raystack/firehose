package com.gojek.esb.sink.redis.parsers;

import com.gojek.esb.metrics.StatsDReporter;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instrumentation
 * <p>
 * Handle instrumentation for {@see RedisParser}. Instrumentation can be in form
 * of log or StatsD Metric.
 */
public class Instrumentation {

  private static final Logger LOGGER = LoggerFactory.getLogger(Instrumentation.class);
  private StatsDReporter statsDReporter;

  public Instrumentation(StatsDReporter statsDReporter) {
    this.statsDReporter = statsDReporter;
  }

  public void captureReadingKeyError(InvalidProtocolBufferException e) {
    // TODO send the error message to statsdReporter FATAL
    LOGGER.error("Unable to parse data when reading Key");
  }

  public void captureInvalidTemplateError(String template, IllegalArgumentException invalidTemplateException) {
    // TODO send the error message to statsdReporter FATAL
    LOGGER.error(String.format("Template '%s' is invalid", template));
  }

  public void captureEmptyKeyError(String template, InvalidConfigurationException emptyKeyException) {
    // TODO send the error message to statsdRepoter FATAL
    LOGGER.error(String.format("Empty key configuration: '%s'", template));
  }

  public void captureDescritptorNotFoundError(String fieldNumber, IllegalArgumentException descriptorNotFoundException) {
    // TODO send the error message to statsdReporter FATAL
    LOGGER.error(String.format("Descriptor not found for index: %s", fieldNumber));
  }

  public void captureProtoIndexNotFoundException(IllegalArgumentException illegalArgumentException) {
    // TODO send the error message to ostatsdReporoter with FATAL
  }
}
