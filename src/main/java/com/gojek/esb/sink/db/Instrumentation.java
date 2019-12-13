package com.gojek.esb.sink.db;

import static com.gojek.esb.metrics.Metrics.DB_SINK_MESSAGES_COUNT;
import static com.gojek.esb.metrics.Metrics.DB_SINK_WRITE_TIME;
import static com.gojek.esb.metrics.Metrics.FAILURE_TAG;
import static com.gojek.esb.metrics.Metrics.SUCCESS_TAG;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.StatsDReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instrumentation
 */
class Instrumentation {

  private static final Logger LOGGER = LoggerFactory.getLogger(Instrumentation.class);
  private StatsDReporter statsDReporter;
  private Instant startExecutionTimestamp;

  public Instrumentation(StatsDReporter statsDReporter) {
    this.statsDReporter = statsDReporter;
  }

  public void startExecution() {
    startExecutionTimestamp = statsDReporter.getClock().now();
  }

  public void captureSuccessAtempt(List<EsbMessage> esbMessages) {
    statsDReporter.captureDurationSince(DB_SINK_WRITE_TIME, startExecutionTimestamp);
    statsDReporter.captureCount(DB_SINK_MESSAGES_COUNT, esbMessages.size(), SUCCESS_TAG);
  }

  public void captureFailedAttempt(SQLException sqlException, List<EsbMessage> esbMessages) {
    // TODO mark as NON-FATAL error
    LOGGER.error("caught {} {}", sqlException.getClass(), sqlException.getMessage());
    statsDReporter.captureCount(DB_SINK_MESSAGES_COUNT, esbMessages.size(), FAILURE_TAG);
  }

}
