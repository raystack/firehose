package com.gojek.esb.sink.elasticsearch;

import com.gojek.esb.metrics.StatsDReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.AllArgsConstructor;

import static com.gojek.esb.metrics.Metrics.ERROR_MESSAGE_TAG;
import static com.gojek.esb.metrics.Metrics.FATAL_ERROR;
import static com.gojek.esb.metrics.Metrics.ERROR_EVENT;

/**
 * Instrumentation
 * <p>
 * Handle instrumentation for {@see ESSinkClient}. Instrumentation can be in form
 * of log or StatsD Metric.
 */

@AllArgsConstructor
public class Instrumentation {

  private static final Logger LOGGER = LoggerFactory.getLogger(Instrumentation.class);
  private StatsDReporter statsDReporter;

  public void captureInvalidConfiguration(IllegalArgumentException illegalArgumentException) {
    LOGGER.error(illegalArgumentException.getMessage());
    statsDReporter.recordEvent(ERROR_EVENT, FATAL_ERROR, errorTag(illegalArgumentException, FATAL_ERROR));
  }

  private String errorTag(Exception e, String errorType) {
    return ERROR_MESSAGE_TAG + "=" + e.getClass().getName() + ",type=" + errorType;
  }
}
