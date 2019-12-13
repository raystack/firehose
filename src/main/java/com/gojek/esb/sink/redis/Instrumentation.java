package com.gojek.esb.sink.redis;

import static com.gojek.esb.metrics.Metrics.REDIS_SINK_MESSAGES_COUNT;
import static com.gojek.esb.metrics.Metrics.REDIS_SINK_WRITE_TIME;
import static com.gojek.esb.metrics.Metrics.SUCCESS_TAG;

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

  StatsDReporter statsDReporter;
  private Instant startExecutionTimestamp;
  private static final Logger LOGGER = LoggerFactory.getLogger(Instrumentation.class);

  public Instrumentation(StatsDReporter statsDReporter) {
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
    // TODO capture as FATAL ERROR to statsdReporter
    LOGGER.error("Redis Pipeline error: no responds received");
  }
}
