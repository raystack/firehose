package com.gojek.esb.metrics;

public class Metrics {

    //HTTP SINK
    public static final String HTTP_EXECUTION_TIME = "http.execution_time";
    public static final String HTTP_RESPONSE_CODE = "http.response_code";

    //DB SINK
    public static final String DB_SINK_WRITE_TIME = "db.sink.write.time";
    public static final String DB_SINK_MESSAGES_COUNT = "db.sink.messages.count";

    //INFLUX DB SINK
    public static final String INFLUX_DB_SINK_WRITE_TIME = "influx.db.sink.write.time";
    public static final String INFLUX_DB_SINK_MESSAGES_COUNT = "influx.db.sink.messages.count";


    //KAFKA
    public static final String KAFKA_PREFIX = "kafka.";
    public static final String KAFKA_FILTERED_MESSAGE = KAFKA_PREFIX + "filtered";
    public static final String PARTITION_PROCESS_TIME = KAFKA_PREFIX + "process_partitions_time";
    public static final String MESSAGE_RECEIVED = KAFKA_PREFIX + "messages.received";
    public static final String KAFKA_COMMIT_COUNT = KAFKA_PREFIX + "commit.async.count";


    //RETRY
    public static final String RETRY_QUEUE_PREFIX = "retry.queue.";
    public static final String RETRY_ATTEMPTS = RETRY_QUEUE_PREFIX + "attempts";
    public static final String RETRY_MESSAGE_COUNT = RETRY_QUEUE_PREFIX + "messages.count";
    public static final String REQUEST_RETRY = "request_retries";


    //TAGS
    public static final String SUCCESS_TAG = "success=true";
    public static final String FAILURE_TAG = "success=false";

}
