package com.gojek.esb.metrics;

public class Metrics {

    //KAFKA
    public static final String KAFKA_PREFIX = "kafka.";
    public static final String KAFKA_FILTERED_MESSAGE = KAFKA_PREFIX + "filtered";
    public static final String PARTITION_PROCESS_TIME = KAFKA_PREFIX + "process_partitions_time";
    public static final String MESSAGE_RECEIVED = KAFKA_PREFIX + "messages.received";
    public static final String KAFKA_COMMIT_COUNT = KAFKA_PREFIX + "commit.async.count";

    //TELEMETRY MEASUREMENTS
    public static final String LIFETIME_TILL_SINK = "lifetime.till.sink";
    public static final String RESPONSE_TIME = "response.time";
    public static final String MESSAGE_COUNT = "messages.count";
    public static final String LATENCY_ACROSS_FIREHOSE = "latency";

    //RETRY
    public static final String RETRY_QUEUE_PREFIX = "retry.queue.";
    public static final String RETRY_ATTEMPTS = RETRY_QUEUE_PREFIX + "attempts";
    public static final String RETRY_MESSAGE_COUNT = RETRY_QUEUE_PREFIX + "messages.count";
    public static final String REQUEST_RETRY = "request_retries";



    //TAGS
    public static final String SUCCESS_TAG = "success=true";
    public static final String FAILURE_TAG = "success=false";

    //ES
    public static final String ES_SINK_PROCESSING_TIME = "es.sink.batch_processing_time";
    public static final String ES_SINK_FAILED_DOCUMENT_COUNT = "es.sink.failed_document_count";
    public static final String ES_SINK_SUCCESS_DOCUMENT_COUNT = "es.sink.success_document_count";
    public static final String ES_SINK_BATCH_FAILURE_COUNT = "es.sink.batch_failure_count";

    // ERROR
    public static final String ERROR_MESSAGE_TAG = "class";
    public static final String ERROR_EVENT = "error.event";
    public static final String NON_FATAL_ERROR = "NON_FATAL_ERROR";
    public static final String FATAL_ERROR = "FATAL_ERROR";
}
