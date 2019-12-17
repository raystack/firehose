package com.gojek.esb.sink.redis.parsers;

import com.gojek.esb.metrics.StatsDReporter;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.gojek.esb.metrics.Metrics.FATAL_ERROR;
import static com.gojek.esb.metrics.Metrics.ERROR_MESSAGE_TAG;
import static com.gojek.esb.metrics.Metrics.ERROR_EVENT;

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
    LOGGER.error("Unable to parse data when reading Key");
    this.captureFatalError(e);
  }

  public void captureInvalidTemplateError(String template, IllegalArgumentException invalidTemplateException) {
    LOGGER.error(String.format("Template '%s' is invalid", template));
    this.captureFatalError(invalidTemplateException);
  }

  public void captureEmptyKeyError(String template, InvalidConfigurationException emptyKeyException) {
    LOGGER.error(String.format("Empty key configuration: '%s'", template));
    this.captureFatalError(emptyKeyException);
  }

  public void captureDescritptorNotFoundError(String fieldNumber, IllegalArgumentException descriptorNotFoundException) {
    LOGGER.error(String.format("Descriptor not found for index: %s", fieldNumber));
    this.captureFatalError(descriptorNotFoundException);
  }

  public void captureProtoIndexNotFoundException(IllegalArgumentException illegalArgumentException) {
    captureFatalError(illegalArgumentException);
  }

  private void captureFatalError(Exception e) {
    statsDReporter.recordEvent(ERROR_EVENT, FATAL_ERROR, errorTag(e, FATAL_ERROR));
  }

  private String errorTag(Exception e, String errorType) {
    return ERROR_MESSAGE_TAG + "=" + e.getClass().getName() + ",type=" + errorType;
  }
}
