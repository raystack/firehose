package com.gojek.esb.metrics;

public class Metrics {
    //SOURCE PREFIXES
    public static final String SOURCE_PREFIX = "source_";
    public static final String KAFKA_PREFIX = "kafka_";

    //SINK PREFIXES
    public static final String SINK_PREFIX = "sink_";
    public static final String HTTP_SINK_PREFIX = "http_";

    //DLQ PREFIX
    public static final String DLQ_PREFIX = "dlq_";

    //PIPELINE PREFIX
    public static final String PIPELINE_PREFIX = "pipeline_";

    //ERROR PREFIX
    public static final String ERROR_PREFIX = "error_";

    // SOURCE MEASUREMENTS
    public static final String SOURCE_KAFKA_MESSAGES_FILTER_COUNT = SOURCE_PREFIX + KAFKA_PREFIX + "messages_filter_count";
    public static final String SOURCE_KAFKA_MESSAGES_COMMIT_COUNT = SOURCE_PREFIX + KAFKA_PREFIX + "messages_commit_count";
    public static final String SOURCE_KAFKA_PARTITIONS_PROCESS_TIME = SOURCE_PREFIX + KAFKA_PREFIX + "partitions_process_time";
    public static final String SOURCE_KAFKA_PULL_BATCH_SIZE = SOURCE_PREFIX + KAFKA_PREFIX + "pull_batch_size";

    // SINK MEASUREMENTS
    public static final String SINK_MESSAGES_COUNT = SINK_PREFIX + "messages_count";
    public static final String SINK_RESPONSE_TIME = SINK_PREFIX + "response_time";
    public static final String SINK_MESSAGES_DROP_COUNT = SINK_PREFIX + "messages_drop_count";
    public static final String SINK_HTTP_RESPONSE_CODE = SINK_PREFIX + HTTP_SINK_PREFIX + "response_code";
    public static final String SINK_PUSH_BATCH_SIZE = SINK_PREFIX + "push_batch_size";

    // RETRY MEASUREMENT
    public static final String RETRY_COUNT = "retry_count";

    // DLQ MEASUREMENTS
    public static final String DQL_RETRY_COUNT = DLQ_PREFIX + "retry_count";
    public static final String DLQ_MESSAGES_COUNT = DLQ_PREFIX + "messages_count";

    // PIPELINE DURATION MEASUREMENTS
    public static final String PIPELINE_END_LATENCY = PIPELINE_PREFIX + "end_latency";
    public static final String PIPELINE_EXECUTION_LIFETIME = PIPELINE_PREFIX + "execution_lifetime";

    // ERROR MEASUREMENT
    public static final String ERROR_EVENT = ERROR_PREFIX + "event";

    // EXECUTION TAGS
    public static final String SUCCESS_TAG = "success=true";
    public static final String FAILURE_TAG = "success=false";

    // ERROR TAGS
    public static final String ERROR_MESSAGE_CLASS_TAG = "class";
    public static final String NON_FATAL_ERROR = "nonfatal";
    public static final String FATAL_ERROR = "fatal";
}
