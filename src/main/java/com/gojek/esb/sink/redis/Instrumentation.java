package com.gojek.esb.sink.redis;

import static com.gojek.esb.metrics.Metrics.REDIS_SINK_MESSAGES_COUNT;
import static com.gojek.esb.metrics.Metrics.REDIS_SINK_WRITE_TIME;
import static com.gojek.esb.metrics.Metrics.SUCCESS_TAG;
import static com.gojek.esb.metrics.Metrics.FATAL_ERROR;
import static com.gojek.esb.metrics.Metrics.ERROR_EVENT;
import static com.gojek.esb.metrics.Metrics.ERROR_MESSAGE_TAG;

import java.time.Instant;

import com.gojek.esb.metrics.StatsDReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instrumentation
 * <p>
 * Handle instrumentation for {@see RedisSink}. Instrumentation can be in form
 * of log or StatsD Metric.
 */
class Instrumentation {

  private StatsDReporter statsDReporter;
  private Instant startExecutionTimestamp;
  private static final Logger LOGGER = LoggerFactory.getLogger(Instrumentation.class);

  Instrumentation(StatsDReporter statsDReporter) {
    this.statsDReporter = statsDReporter;
  }

  /**
   * startExecution
   * <p>
   * This method get the start time of execution. The time will be used by
   * {@see captureExecutionTelemetry} be substracted with time when execution
   * ended. This delta time will be sent as {@see REDIS_SINK_WRITE_TIME}
   */
  public void startExecution() {
    startExecutionTimestamp = statsDReporter.getClock().now();
  }

  public void captureExecutionTelemetry(int processedMessageSize) {
    LOGGER.info("Pushed {} messages to redis.", processedMessageSize);
    statsDReporter.captureDurationSince(REDIS_SINK_WRITE_TIME, startExecutionTimestamp);
    statsDReporter.captureCount(REDIS_SINK_MESSAGES_COUNT, processedMessageSize, SUCCESS_TAG);
  }

  public void captureClientError() {
    String message = "Redis Pipeline error: no responds received";
    LOGGER.error(message);
    statsDReporter.recordEvent(ERROR_EVENT, FATAL_ERROR, errorTag(new RuntimeException(), FATAL_ERROR));
  }

  private String errorTag(Exception e, String errorType) {
    return ERROR_MESSAGE_TAG + "=" + e.getClass().getName() + ",type=" + errorType;
  }
}
