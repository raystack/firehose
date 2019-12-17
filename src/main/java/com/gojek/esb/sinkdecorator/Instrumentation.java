package com.gojek.esb.sinkdecorator;

import static com.gojek.esb.metrics.Metrics.FAILURE_TAG;
import static com.gojek.esb.metrics.Metrics.RETRY_MESSAGE_COUNT;
import static com.gojek.esb.metrics.Metrics.SUCCESS_TAG;
import static com.gojek.esb.metrics.Metrics.RETRY_ATTEMPTS;
import static com.gojek.esb.metrics.Metrics.ERROR_EVENT;
import static com.gojek.esb.metrics.Metrics.NON_FATAL_ERROR;
import static com.gojek.esb.metrics.Metrics.ERROR_MESSAGE_TAG;

import java.util.List;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.StatsDReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.AllArgsConstructor;

/**
 * Instrumentation
 * <p>
 * Handle instrumentation for {@see SinkWithRetryQueue}. Instrumentation can be in form
 * of log or StatsD Metric.
 */
@AllArgsConstructor
class Instrumentation {

  private static final Logger LOGGER = LoggerFactory.getLogger(Instrumentation.class);
  private StatsDReporter statsDReporter;

  public void captureRetryAttempts() {
    statsDReporter.increment(RETRY_ATTEMPTS);
  }

  public void captureMessagesPushedToRetryQueue(List<EsbMessage> failedMessages, String topic) {
    LOGGER.info("Pushing {} messages to retry queue topic : {}", failedMessages.size(), topic);
  }

  public void incrementMessageFailCount(EsbMessage message, Exception e) {
    statsDReporter.increment(RETRY_MESSAGE_COUNT, FAILURE_TAG);
    LOGGER.error("Unable to send record with key " + message.getLogKey() + " and message " + message.getLogMessage(), e);
    caputreNonFatalError(e);
  }

  public void incrementMessageSucceedCount() {
    statsDReporter.increment(RETRY_MESSAGE_COUNT, SUCCESS_TAG);
  }

  public void capturePushToRetryQueueError(InterruptedException e) {
    LOGGER.error(e.getMessage(), e.getClass());
    caputreNonFatalError(e);
  }

  public void capturePushToRetryQueueSuccess(List<EsbMessage> failedMessages, List<EsbMessage> retryMessages, String topic) {
    LOGGER.info("Successfully pushed {} messages to {}", failedMessages.size() - retryMessages.size(), topic);
  }

  /**
   *  ================ BackOff Instrumentation ===================
   * Currently only one method needed for instrumenting BackOff class.
   * Can be extracted out later when it grows.
   * */

  public void captureBackOffThreadInteruptedError(InterruptedException e,
  long milliseconds) {
    LOGGER.error("Backoff thread sleep for {} milliseconds interrupted : {} {}",
    milliseconds, e.getClass(), e.getMessage());
    caputreNonFatalError(e);
  }

  private void caputreNonFatalError(Exception e) {
    statsDReporter.recordEvent(ERROR_EVENT, NON_FATAL_ERROR, errorTag(e, NON_FATAL_ERROR));
  }

  private String errorTag(Exception e, String errorType) {
    return ERROR_MESSAGE_TAG + "=" + e.getClass().getName() + ",type=" + errorType;
  }
}
