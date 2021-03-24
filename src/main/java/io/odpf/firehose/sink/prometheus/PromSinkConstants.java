package io.odpf.firehose.sink.prometheus;

public class PromSinkConstants {

    public static final String CONTENT_ENCODING = "Content-Encoding";
    public static final String PROMETHEUS_REMOTE_WRITE_VERSION = "X-Prometheus-Remote-Write-Version";
    public static final String CONTENT_ENCODING_DEFAULT = "snappy";
    public static final String PROMETHEUS_REMOTE_WRITE_VERSION_DEFAULT = "0.1.0";

    public static final String FIELD_NAME_MAPPING_ERROR_MESSAGE = "field index mapping cannot be empty; at least one field value is required";

    public static final String DEFAULT_LABEL_NAME = "__name__";
    public static final String METRIC_NAME = "metric_name";
    public static final String METRIC_VALUE = "metric_value";
    public static final String KAFKA_PARTITION = "kafka_partition";

    public static final long SECONDS_SCALED_TO_MILLI = 1000L;
    public static final long MILLIS_SCALED_TO_NANOS = 1000000L;

    static final String SUCCESS_CODE_PATTERN = "^2.*";
}
