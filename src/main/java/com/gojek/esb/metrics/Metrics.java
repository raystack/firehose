package com.gojek.esb.metrics;

public class Metrics {

    //KAFKA
    public static final String KAFKA_PREFIX = "kafka.";
    public static final String KAFKA_FILTERED_MESSAGE = KAFKA_PREFIX + "filtered";
    public static final String PARTITION_PROCESS_TIME = KAFKA_PREFIX + "process_partitions_time";
    public static final String KAFKA_COMMIT_COUNT = KAFKA_PREFIX + "commit.async.count";

    //TELEMETRY MEASUREMENTS
    public static final String LIFETIME_TILL_EXECUTION = "lifetime.till.execution";
    public static final String SINK_RESPONSE_TIME = "sink.response.time";
    public static final String MESSAGE_COUNT = "messages.count";
    public static final String MESSAGES_DROPPED_COUNT = "messages.dropped.count";
    public static final String LATENCY_ACROSS_FIREHOSE = "latency";
    public static final String HTTP_RESPONSE_CODE = "http.response.code";
    public static final String PULLED_BATCH_SIZE = "pulled.batch.size";
    public static final String PUSHED_BATCH_SIZE = "pushed.batch.size";

    //RETRY
    public static final String RETRY_QUEUE_PREFIX = "retry.queue.";
    public static final String RETRY_ATTEMPTS = RETRY_QUEUE_PREFIX + "attempts";
    public static final String RETRY_MESSAGE_COUNT = RETRY_QUEUE_PREFIX + "messages.count";
    public static final String REQUEST_RETRY = "request_retries";

    //TAGS
    public static final String SUCCESS_TAG = "success=true";
    public static final String FAILURE_TAG = "success=false";

    // ERROR
    public static final String ERROR_MESSAGE_TAG = "class";
    public static final String ERROR_EVENT = "error.event";
    public static final String NON_FATAL_ERROR = "NON_FATAL_ERROR";
    public static final String FATAL_ERROR = "FATAL_ERROR";
}
