package com.gojek.esb.sink.elasticsearch;

import com.gojek.esb.metrics.StatsDReporter;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static com.gojek.esb.metrics.Metrics.*;


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
            statsDReporter.captureDurationSince(RESPONSE_TIME, getStartTime(), FAILURE_TAG);
            statsDReporter.captureCount(ES_SINK_FAILED_DOCUMENT_COUNT, failedCount, FAILURE_TAG);
            statsDReporter.captureCount(ES_SINK_SUCCESS_DOCUMENT_COUNT, (response.getItems().length - failedCount), SUCCESS_TAG);
            statsDReporter.captureCount(ES_SINK_BATCH_FAILURE_COUNT, 1, FAILURE_TAG);
        } else {
            LOGGER.debug("Bulk [{}] completed in {} milliseconds",
                    executionId, response.getTook().getMillis());

            statsDReporter.captureDurationSince(RESPONSE_TIME, getStartTime(), SUCCESS_TAG);
            statsDReporter.captureCount(ES_SINK_SUCCESS_DOCUMENT_COUNT, response.getItems().length, SUCCESS_TAG);
        }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        LOGGER.error("Failed to execute bulk", failure);
        statsDReporter.recordEvent(ERROR_EVENT, NON_FATAL_ERROR, errorTag(failure, NON_FATAL_ERROR));

        statsDReporter.captureDurationSince(RESPONSE_TIME, getStartTime(), FAILURE_TAG);
        statsDReporter.captureCount(ES_SINK_FAILED_DOCUMENT_COUNT, request.numberOfActions(), FAILURE_TAG);
        statsDReporter.captureCount(ES_SINK_BATCH_FAILURE_COUNT, 1, FAILURE_TAG);
    }

    private String errorTag(Throwable e, String errorType) {
        return ERROR_MESSAGE_TAG + "=" + e.getClass().getName() + ",type=" + errorType;
    }
}
