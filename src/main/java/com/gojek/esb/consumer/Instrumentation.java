package com.gojek.esb.consumer;

import com.gojek.esb.metrics.StatsDReporter;

import static com.gojek.esb.metrics.Metrics.KAFKA_FILTERED_MESSAGE;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instrumentation
 * <p>
 * Handle instrumentation for {@see com.gojek.esb.consumer}. Instrumentation can be in form
 * of log or StatsD Metric.
 */
public class Instrumentation {

  private Logger logger;
  private StatsDReporter statsDReporter;

  public Instrumentation(StatsDReporter statsDReporter, Class clazz) {
    this.statsDReporter = statsDReporter;
    this.logger = LoggerFactory.getLogger(Instrumentation.class);
  }

  public void logConsumedRecordsCount(ConsumerRecords records) {
    logger.info("Pulled {} messages", records.count());
  }

  public void logRecord(ConsumerRecord<byte[], byte[]> record) {
    logger.debug("Pulled record: {}", record);
  }

  public void captureFilteredMessageCount(int filteredMessageCount, String filterExpression) {
    statsDReporter.captureCount(KAFKA_FILTERED_MESSAGE, filteredMessageCount, "expr=" + filterExpression);
  }

  public void captureClosingError(Exception e) {
    // TODO add to non fatal
    logger.error("Exception while closing ", e);
  }
}
