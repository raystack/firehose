package com.gojek.esb.sinkdecorator;

import static com.gojek.esb.metrics.Metrics.FAILURE_TAG;
import static com.gojek.esb.metrics.Metrics.RETRY_MESSAGE_COUNT;
import static com.gojek.esb.metrics.Metrics.SUCCESS_TAG;
import static com.gojek.esb.metrics.Metrics.RETRY_ATTEMPTS;

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
    // TODO add to non fatal error
    statsDReporter.increment(RETRY_MESSAGE_COUNT, FAILURE_TAG);
    LOGGER.error("Unable to send record with key " + message.getLogKey() + " and message " + message.getLogMessage(), e);
  }

  public void incrementMessageSucceedCount() {
    statsDReporter.increment(RETRY_MESSAGE_COUNT, SUCCESS_TAG);
  }

  public void capturePushToRetryQueueError(InterruptedException e) {
    // TODO add to non fatal error
    LOGGER.error(e.getMessage(), e.getClass());
  }

  public void capturePushToRetryQueueSuccess(List<EsbMessage> failedMessages, List<EsbMessage> retryMessages, String topic) {
    LOGGER.info("Successfully pushed {} messages to {}", failedMessages.size() - retryMessages.size(), topic);
  }
}
