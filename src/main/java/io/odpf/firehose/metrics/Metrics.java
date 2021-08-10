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
    public static final String OBJECTSTORAGE_SINK_PREFIX = "objectstorage_";

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

    // OBJECT STORAGE SINK MEASUREMENTS
    public static final String SINK_OBJECTSTORAGE_RECORD_PROCESSED_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + OBJECTSTORAGE_SINK_PREFIX + "record_processed_total";
    public static final String SINK_OBJECTSTORAGE_RECORD_PROCESSING_FAILED_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + OBJECTSTORAGE_SINK_PREFIX + "record_processing_failed_total";

    public static final String SINK_OBJECTSTORAGE_LOCAL_FILE_OPEN_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + OBJECTSTORAGE_SINK_PREFIX + "file_open_total";
    public static final String SINK_OBJECTSTORAGE_LOCAL_FILE_CLOSE_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + OBJECTSTORAGE_SINK_PREFIX + "file_close_total";
    public static final String SINK_OBJECTSTORAGE_LOCAL_FILE_CLOSING_TIME_MILLISECONDS = APPLICATION_PREFIX + SINK_PREFIX + OBJECTSTORAGE_SINK_PREFIX + "file_closing_time_milliseconds";
    public static final String SINK_OBJECTSTORAGE_LOCAL_FILE_SIZE_BYTES = APPLICATION_PREFIX + SINK_PREFIX + OBJECTSTORAGE_SINK_PREFIX + "local_file_size_bytes";

    public static final String SINK_OBJECTSTORAGE_FILE_UPLOAD_TIME_MILLISECONDS = APPLICATION_PREFIX + SINK_PREFIX + OBJECTSTORAGE_SINK_PREFIX + "file_upload_time_milliseconds";
    public static final String SINK_OBJECTSTORAGE_FILE_UPLOAD_TOTAL = APPLICATION_PREFIX + SINK_PREFIX + OBJECTSTORAGE_SINK_PREFIX + "file_upload_total";
    public static final String SINK_OBJECTSTORAGE_FILE_UPLOAD_BYTES = APPLICATION_PREFIX + SINK_PREFIX + OBJECTSTORAGE_SINK_PREFIX + "file_upload_bytes";


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

    // KAFKA TAG
    public static final String TOPIC_TAG = "topic";

    //SINK OBJECT STORAGE TAG
    public static final String PARTITION_TAG = "partition=";
    public static final String SCOPE_TAG = "scope";

    public static final String SINK_OBJECT_STORAGE_SCOPE_FILE_WRITE = "file_write";
    public static final String SINK_OBJECT_STORAGE_SCOPE_FILE_CLOSE = "file_close";
    public static final String SINK_OBJECT_STORAGE_SCOPE_FILE_UPLOAD = "file_upload";

    public static String tag(String key, String value) {
        return String.format("%s=%s", key, value);
    }
}
