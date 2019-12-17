package com.gojek.esb.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.gojek.esb.metrics.Metrics.ERROR_EVENT;
import static com.gojek.esb.metrics.Metrics.ERROR_MESSAGE_TAG;
import static com.gojek.esb.metrics.Metrics.NON_FATAL_ERROR;
import static com.gojek.esb.metrics.Metrics.FATAL_ERROR;
import static com.gojek.esb.metrics.Metrics.KAFKA_FILTERED_MESSAGE;

/**
 * Instrumentation
 * <p>
 * Handle logging and metric capturing.
 */
public class Instrumentation {

  private StatsDReporter statsDReporter;
  private Logger logger;

  public Instrumentation(StatsDReporter statsDReporter, Logger logger) {
    this.statsDReporter = statsDReporter;
    this.logger = logger;
  }

  public Instrumentation(StatsDReporter statsDReporter, Class clazz) {
    this.statsDReporter = statsDReporter;
    this.logger = LoggerFactory.getLogger(clazz);
  }

  public void logInfo(String message) {
    logger.info(message);
  }

  public void logInfo(String template, Object... t) {
    logger.info(template, t);
  }

  public void logDebug(String message, Object arg1) {
    logger.debug(message, arg1);
  }

  public void captureFilteredMessageCount(int filteredMessageCount, String filterExpression) {
    statsDReporter.captureCount(KAFKA_FILTERED_MESSAGE, filteredMessageCount, "expr=" + filterExpression);
  }

  public void captureNonFatalError(Exception e, String message) {
    logger.warn(message, e);
    String errorTag = ERROR_MESSAGE_TAG + e;
    statsDReporter.recordEvent(ERROR_EVENT, NON_FATAL_ERROR, errorTag);
  }

  public void captureNonFatalError(Exception e, String template, Object... t) {
    logger.warn(template, t);
    String errorTag = ERROR_MESSAGE_TAG + e;
    statsDReporter.recordEvent(ERROR_EVENT, NON_FATAL_ERROR, errorTag);
  }

  public void captureFatalError(Exception e, String message) {
    logger.error(message, e);
    statsDReporter.recordEvent(ERROR_EVENT, FATAL_ERROR, errorTag(e, FATAL_ERROR));
  }

  private String errorTag(Exception e, String errorType) {
    return ERROR_MESSAGE_TAG + "=" + e.getClass().getName() + ",type=" + errorType;
  }

}
