package com.gojek.esb.metrics;

import java.time.Instant;
import java.util.List;

import com.gojek.esb.consumer.EsbMessage;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.gojek.esb.metrics.Metrics.*;

/**
 * Instrumentation
 * <p>
 * Handle logging and metric capturing.
 */
public class Instrumentation {

  private StatsDReporter statsDReporter;
  private Logger logger;
  private Instant startExecutionTime;

  public Instrumentation(StatsDReporter statsDReporter, Logger logger) {
    this.statsDReporter = statsDReporter;
    this.logger = logger;
  }

  public Instrumentation(StatsDReporter statsDReporter, Class clazz) {
    this.statsDReporter = statsDReporter;
    this.logger = LoggerFactory.getLogger(clazz);
  }

  // =================== LOGGING ===================

  public void logInfo(String message) {
    logger.info(message);
  }

  public void logInfo(String template, Object... t) {
    logger.info(template, t);
  }

  public void logDebug(String template, Object... t) {
    logger.debug(template, t);
  }

  // ============== FILTER MESSAGES ==============

  public void captureFilteredMessageCount(int filteredMessageCount, String filterExpression) {
    statsDReporter.captureCount(KAFKA_FILTERED_MESSAGE, filteredMessageCount, "expr=" + filterExpression);
  }

  // =================== ERROR ===================

  public void captureNonFatalError(Exception e) {
    logger.warn(e.getMessage());
    statsDReporter.recordEvent(ERROR_EVENT, NON_FATAL_ERROR, errorTag(e, NON_FATAL_ERROR));
  }

  public void captureNonFatalError(Exception e, String message) {
    logger.warn(message, e);
    captureNonFatalError(e);
  }

  public void captureNonFatalError(Exception e, String template, Object... t) {
    logger.warn(template, t);
    captureNonFatalError(e);
  }

  public void captureFatalError(Exception e) {
    logger.error(e.getMessage());
    statsDReporter.recordEvent(ERROR_EVENT, FATAL_ERROR, errorTag(e, FATAL_ERROR));
  }

  public void captureFatalError(Exception e, String message) {
    logger.error(message, e);
    this.captureFatalError(e);
  }

  public void captureFatalError(Exception e, String template, Object... t) {
    logger.error(template, t);
    this.captureFatalError(e);
  }

  private String errorTag(Exception e, String errorType) {
    return ERROR_MESSAGE_TAG + "=" + e.getClass().getName() + ",type=" + errorType;
  }

  // ================ SinkExecutionTelemetry ================

  public void startExecution() {
    startExecutionTime = statsDReporter.getClock().now();
  }

  private String getWriteTimeMetric(String sinkType) {
    return sinkType + ".sink.write.time";
  }

  private String getMessagesCountMetric(String sinkType) {
    return sinkType + ".sink.messages.count";
  }

  public void captureSuccessExecutionTelemetry(String sinkType, List<EsbMessage> esbMessages) {
    logger.info("Pushed {} messages to {}.", esbMessages.size(), sinkType);
    statsDReporter.captureDurationSince(getWriteTimeMetric(sinkType), this.startExecutionTime);
    statsDReporter.captureCount(getMessagesCountMetric(sinkType), esbMessages.size(), SUCCESS_TAG);
  }

  public void captureFailedExecutionTelemetry(String sinkType, Exception e, List<EsbMessage> esbMessages) {
    captureNonFatalError(e, "caught {} {}", e.getClass(), e.getMessage());
    statsDReporter.captureCount(getMessagesCountMetric(sinkType), esbMessages.size(), FAILURE_TAG);
  }

  // =================== RetryTelemetry ======================

  public void incrementMessageSucceedCount() {
    statsDReporter.increment(RETRY_MESSAGE_COUNT, SUCCESS_TAG);
  }

  public void captureRetryAttempts() {
    statsDReporter.increment(RETRY_ATTEMPTS);
  }

  public void incrementMessageFailCount(EsbMessage message, Exception e) {
    statsDReporter.increment(RETRY_MESSAGE_COUNT, FAILURE_TAG);
    captureNonFatalError(e, "Unable to send record with key {} and message {} ", message.getLogKey(), message.getLogMessage());
  }

  // ===================== LifetimeTillSink =====================

  public void lifetimeTillSink(String sinkType, List<EsbMessage> messages) {
    messages.forEach(message -> {
      statsDReporter.captureDurationSince(getLifeTimeMetric(sinkType), Instant.ofEpochMilli(message.getTimestamp()));
    });
  }

  private String getLifeTimeMetric(String sinkType) {
    return sinkType + ".lifetimeTillSink";
  }

  // ===================== MessageCountTelemetry =================

  public void captureHttpStatusCount(HttpEntityEnclosingRequestBase batchPutMethod, HttpResponse response) {
    String urlTag = "url=" + batchPutMethod.getURI().getPath();
    String httpCodeTag = "status_code=";
    if (response != null) {
      httpCodeTag = "status_code=" + Integer.toString(response.getStatusLine().getStatusCode());
    }
    statsDReporter.captureCount(HTTP_RESPONSE_CODE, 1, httpCodeTag, urlTag);
  }
}
