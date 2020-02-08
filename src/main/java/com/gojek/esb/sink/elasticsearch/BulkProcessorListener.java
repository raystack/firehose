package com.gojek.esb.sink.elasticsearch;

import com.gojek.esb.metrics.StatsDReporter;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;


public class BulkProcessorListener implements BulkProcessor.Listener {

    private final StatsDReporter statsDReporter;
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkProcessorListener.class.getName());

    private Instant startTime;

    private Instant getStartTime() {
        return startTime;
    }

    public BulkProcessorListener(StatsDReporter statsDReporter) {
        this.statsDReporter = statsDReporter;
    }

    private void setStartTime(Instant startTime) {
        this.startTime = startTime;
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
        int numberOfActions = request.numberOfActions();
        LOGGER.debug("Executing bulk [{}] with {} requests",
                executionId, numberOfActions);
        setStartTime(Instant.now());
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        if (response.hasFailures()) {
            LOGGER.warn("Bulk [{}] executed with failures", executionId);
            LOGGER.debug("Failure message is [{}]", response.buildFailureMessage());
            BulkItemResponse[] items = response.getItems();
            int failedCount = 0;
            for (BulkItemResponse responses : items) {
                if (responses.isFailed()) {
                    failedCount += 1;
                    LOGGER.warn("Failure response message [{}]", responses.getFailureMessage());
                }
            }
        } else {
            LOGGER.debug("Bulk [{}] completed in {} milliseconds",
                    executionId, response.getTook().getMillis());

        }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        LOGGER.error("Failed to execute bulk", failure);
    }
}
