package io.odpf.firehose.metrics;

public class Metrics {
    //APPLICATION PREFIX
    public static final String APPLICATION_PREFIX = "firehose_";

    //SOURCE PREFIXES
    public static final String SOURCE_PREFIX = "source_";
    public static final String KAFKA_PREFIX = "kafka_";

    //SINK PREFIXES
    public static final String SINK_PREFIX = "sink_";
    public static final String HTTP_SINK_PREFIX = "http_";
    public static final String MONGO_SINK_PREFIX = "mongo_";


    //RETRY PREFIX
    public static final String RETRY_PREFIX = "retry_";

    //DLQ PREFIX
    public static final String DLQ_PREFIX = "dlq_";

    //PIPELINE PREFIX
    public static final String PIPELINE_PREFIX = "pipeline_";

    //ERROR PREFIX
    public static final String ERROR_PREFIX = "error_";

    // SOURCE MEASUREMENTS
    public static final String SOURCE_KAFKA_MESSAGES_FILTER_TOTAL = APPLICATION_PREFIX + SOURCE_PREFIX + KAFKA_PREFIX + "messages_filter_total";
    public static final String SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL = APPLICATION_PREFIX + SOURCE_PREFIX + KAFKA_PREFIX + "messages_commit_total";
    public static final String SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS = APPLICATION_PREFIX + SOURCE_PREFIX + KAFKA_PREFIX + "partitions_process_milliseconds";
    public static final String SOURCE_KAFKA_PULL_BATCH_SIZE_TOTAL = APPLICATION_PREFIX + SOURCE_PREFIX + KAFKA_PREFIX + "pull_batch_size_total";

    // SINK MEASUREMENTS
    public static final String SINK_MESSAGES_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + "messages_total";
    public static final String SINK_RESPONSE_TIME_MILLISECONDS = APPLICATION_PREFIX + SINK_PREFIX + "response_time_milliseconds";
    public static final String SINK_MESSAGES_DROP_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + "messages_drop_total";
    public static final String SINK_HTTP_RESPONSE_CODE_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + HTTP_SINK_PREFIX + "response_code_total";
    public static final String SINK_PUSH_BATCH_SIZE_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + "push_batch_size_total";

    // MONGO SINK MEASUREMENTS
    public static final String SINK_MONGO_INSERTED_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + MONGO_SINK_PREFIX + "inserted_total";
    public static final String SINK_MONGO_UPDATED_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + MONGO_SINK_PREFIX + "updated_total";
    public static final String SINK_MONGO_MODIFIED_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + MONGO_SINK_PREFIX + "modified_total";

    // RETRY MEASUREMENT
    public static final String RETRY_TOTAL = APPLICATION_PREFIX + RETRY_PREFIX + "total";
    public static final String RETRY_SLEEP_TIME_MILLISECONDS = APPLICATION_PREFIX + RETRY_PREFIX + "backoff_sleep_milliseconds";

    // DLQ MEASUREMENTS
    public static final String DQL_RETRY_TOTAL = APPLICATION_PREFIX + DLQ_PREFIX + "retry_total";
    public static final String DLQ_MESSAGES_TOTAL = APPLICATION_PREFIX + DLQ_PREFIX + "messages_total";

    // PIPELINE DURATION MEASUREMENTS
    public static final String PIPELINE_END_LATENCY_MILLISECONDS = APPLICATION_PREFIX + PIPELINE_PREFIX + "end_latency_milliseconds";
    public static final String PIPELINE_EXECUTION_LIFETIME_MILLISECONDS = APPLICATION_PREFIX + PIPELINE_PREFIX + "execution_lifetime_milliseconds";

    // ERROR MEASUREMENT
    public static final String ERROR_EVENT = APPLICATION_PREFIX + ERROR_PREFIX + "event";

    // EXECUTION TAGS
    public static final String SUCCESS_TAG = "success=true";
    public static final String FAILURE_TAG = "success=false";

    // ERROR TAGS
    public static final String ERROR_MESSAGE_CLASS_TAG = "class";
    public static final String NON_FATAL_ERROR = "nonfatal";
    public static final String FATAL_ERROR = "fatal";
}
