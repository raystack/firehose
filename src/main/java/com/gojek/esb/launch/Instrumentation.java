package com.gojek.esb.launch;

import java.io.IOException;

import com.gojek.esb.metrics.StatsDReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instrumentation
 * <p>
 * Handle instrumentation for {@see Main}. Instrumentation can be in form
 * of log or StatsD Metric.
 */
class Instrumentation {

  private static final Logger LOGGER = LoggerFactory.getLogger(Instrumentation.class);
  private StatsDReporter statsDReporter;

  Instrumentation(StatsDReporter statsDReporter) {
    this.statsDReporter = statsDReporter;
  }

  public void logConsumerThreadInterrupted() {
    LOGGER.info("Consumer Thread interrupted, leaving the loop!");
  }

  public void captureConsumerThreadError(Exception e) {
    // TODO put it into NON-FATAL error
    LOGGER.error("Exception in Consumer Thread {} {} continuing", e.getMessage(), e);
  }

  public void captureConsumerCreationFailure(Exception e) {
    // TODO put it into FATAL error
    LOGGER.error("Exception on creating the consumer, exiting the application", e);
  }

  public void logShutdownHook() {
    LOGGER.info("Executing the shutdown hook");
  }

  public void logExitMainThread() {
    LOGGER.info("Exiting main thread");
  }

  public void captureConsumerCloseError(IOException e) {
    LOGGER.error("Exception on closing firehose consumer", e);
  }
}
