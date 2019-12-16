package com.gojek.esb.sink.elasticsearch;

import com.gojek.esb.metrics.StatsDReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.AllArgsConstructor;

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
    // TODO add to fatal ERROR
    LOGGER.error(illegalArgumentException.getMessage());
  }
}
