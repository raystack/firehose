package com.gojek.esb.sink.db;

import static com.gojek.esb.metrics.Metrics.DB_SINK_MESSAGES_COUNT;
import static com.gojek.esb.metrics.Metrics.DB_SINK_WRITE_TIME;
import static com.gojek.esb.metrics.Metrics.FAILURE_TAG;
import static com.gojek.esb.metrics.Metrics.SUCCESS_TAG;
import static com.gojek.esb.metrics.Metrics.ERROR_EVENT;
import static com.gojek.esb.metrics.Metrics.NON_FATAL_ERROR;
import static com.gojek.esb.metrics.Metrics.ERROR_MESSAGE_TAG;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.StatsDReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instrumentation
 * <p>
 * Handle DBSink logging and capturing metrics.
 */
class Instrumentation {

  private static final Logger LOGGER = LoggerFactory.getLogger(DBSink.class);
  private StatsDReporter statsDReporter;
  private Instant startExecutionTimestamp;

  Instrumentation(StatsDReporter statsDReporter) {
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
    LOGGER.error("caught {} {}", sqlException.getClass(), sqlException.getMessage());
    // String errorTag = ERROR_MESSAGE_TAG + "=" + sqlException.getClass().getName();
    statsDReporter.recordEvent(ERROR_EVENT, NON_FATAL_ERROR, errorTag(sqlException, NON_FATAL_ERROR));
    statsDReporter.captureCount(DB_SINK_MESSAGES_COUNT, esbMessages.size(), FAILURE_TAG);
  }

  private String errorTag(Exception e, String errorType) {
    return ERROR_MESSAGE_TAG + "=" + e.getClass().getName() + ",type=" + errorType;
  }

}
