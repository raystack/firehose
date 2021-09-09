package io.odpf.firehose.metrics;

import io.odpf.firehose.consumer.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static io.odpf.firehose.metrics.Metrics.*;

/**
 * Instrumentation.
 * <p>
 * Handle logging and metric capturing.
 */
public class Instrumentation {

    private StatsDReporter statsDReporter;
    private Logger logger;


    /**
     * Gets start execution time.
     *
     * @return the start execution time
     */
    public Instant getStartExecutionTime() {
        return startExecutionTime;
    }

    private Instant startExecutionTime;

    /**
     * Instantiates a new Instrumentation.
     *
     * @param statsDReporter the stats d reporter
     * @param logger         the logger
     */
    public Instrumentation(StatsDReporter statsDReporter, Logger logger) {
        this.statsDReporter = statsDReporter;
        this.logger = logger;
    }

    /**
     * Instantiates a new Instrumentation.
     *
     * @param statsDReporter the stats d reporter
     * @param clazz          the clazz
     */
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

    public void logWarn(String template, Object... t) {
        logger.warn(template, t);
    }

    public void logDebug(String template, Object... t) {
        logger.debug(template, t);
    }

    public void logError(String template, Object... t) {
        logger.error(template, t);
    }

    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }
    // ============== FILTER MESSAGES ==============

    /**
     * Captures batch message histogram.
     *
     * @param pulledMessageCount the pulled message count
     */
    public void capturePulledMessageHistogram(long pulledMessageCount) {
        statsDReporter.captureHistogram(SOURCE_KAFKA_PULL_BATCH_SIZE_TOTAL, pulledMessageCount);
    }

    /**
     * Captures filtered message count.
     *
     * @param filteredMessageCount the filtered message count
     * @param filterExpression     the filter expression
     */
    public void captureFilteredMessageCount(int filteredMessageCount, String filterExpression) {
        statsDReporter.captureCount(SOURCE_KAFKA_MESSAGES_FILTER_TOTAL, filteredMessageCount, "expr=" + filterExpression);
    }

    // =================== ERROR ===================

    public void captureNonFatalError(Exception e) {
        logger.warn(e.getMessage(), e);
        statsDReporter.recordEvent(ERROR_EVENT, NON_FATAL_ERROR, errorTag(e, NON_FATAL_ERROR));
    }

    public void captureNonFatalError(Exception e, String message) {
        logger.warn(message);
        captureNonFatalError(e);
    }

    public void captureNonFatalError(Exception e, String template, Object... t) {
        logger.warn(template, t);
        captureNonFatalError(e);
    }

    public void captureFatalError(Exception e) {
        logger.error(e.getMessage(), e);
        statsDReporter.recordEvent(ERROR_EVENT, FATAL_ERROR, errorTag(e, FATAL_ERROR));
    }

    public void captureFatalError(Exception e, String message) {
        logger.error(message);
        this.captureFatalError(e);
    }

    public void captureFatalError(Exception e, String template, Object... t) {
        logger.error(template, t);
        this.captureFatalError(e);
    }

    private String errorTag(Exception e, String errorType) {
        return ERROR_MESSAGE_CLASS_TAG + "=" + e.getClass().getName() + ",type=" + errorType;
    }

    // ================ SinkExecutionTelemetry ================

    public void startExecution() {
        startExecutionTime = statsDReporter.getClock().now();
    }

    /**
     * Captures successful executions.
     *
     * @param sinkType        the sink type
     * @param messageListSize the message list size
     */
    public void captureSuccessExecutionTelemetry(String sinkType, Integer messageListSize) {
        logger.info("Pushed {} messages to {}.", messageListSize, sinkType);
        statsDReporter.captureDurationSince(SINK_RESPONSE_TIME_MILLISECONDS, this.startExecutionTime);
        statsDReporter.captureCount(SINK_MESSAGES_TOTAL, messageListSize, SUCCESS_TAG);
        statsDReporter.captureHistogramWithTags(SINK_PUSH_BATCH_SIZE_TOTAL, messageListSize, SUCCESS_TAG);
    }

    /**
     * Captures failed executions.
     *
     * @param exception       the reported exception
     * @param messageListSize the message list size
     */
    public void captureFailedExecutionTelemetry(Exception exception, Integer messageListSize) {

        captureNonFatalError(exception, "caught {} {}", exception.getClass(), exception.getMessage());
        statsDReporter.captureCount(SINK_MESSAGES_TOTAL, messageListSize, FAILURE_TAG);
        statsDReporter.captureHistogramWithTags(SINK_PUSH_BATCH_SIZE_TOTAL, messageListSize, FAILURE_TAG);
    }

    // =================== RetryTelemetry ======================

    public void incrementMessageSucceedCount() {
        statsDReporter.increment(DLQ_MESSAGES_TOTAL, SUCCESS_TAG);
    }

    public void captureRetryAttempts() {
        statsDReporter.increment(DQL_RETRY_TOTAL);
    }

    public void incrementMessageFailCount(Message message, Exception e) {
        statsDReporter.increment(DLQ_MESSAGES_TOTAL, FAILURE_TAG);
        captureNonFatalError(e, "Unable to send record with key {} and message {} ", message.getLogKey(), message.getLogMessage());
    }

    // ===================== Latency / LifetimeTillSink =====================

    public void capturePreExecutionLatencies(List<Message> messages) {
        messages.forEach(message -> {
            statsDReporter.captureDurationSince(PIPELINE_END_LATENCY_MILLISECONDS, Instant.ofEpochMilli(message.getTimestamp()));
            statsDReporter.captureDurationSince(PIPELINE_EXECUTION_LIFETIME_MILLISECONDS, Instant.ofEpochMilli(message.getConsumeTimestamp()));
        });
    }

    public void captureDurationSince(String metric, Instant instant) {
        statsDReporter.captureDurationSince(metric, instant);
    }

    public void captureSleepTime(String metric, int sleepTime) {
        statsDReporter.gauge(metric, sleepTime);
    }

    // ===================== CountTelemetry =================

    public void captureCountWithTags(String metric, Integer count, String... tags) {
        statsDReporter.captureCount(metric, count, tags);
    }

    public void incrementCounterWithTags(String metric, String... tags) {
        statsDReporter.increment(metric, tags);
    }

    public void incrementCounter(String metric) {
        statsDReporter.increment(metric);
    }

    // ===================== closing =================

    public void close() throws IOException {
        statsDReporter.close();
    }
}
