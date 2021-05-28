package io.odpf.firehose.config;

public interface FileSinkConfig extends AppConfig {

    @Key("SINK_FILE_LOCAL_DIRECTORY")
    String getLocalDirectory();

    @Key("SINK_FILE_KAFKA_METADATA_COLUMN_NAME")
    @DefaultValue("")
    String getKafkaMetadataColumnName();

    @Key("SINK_FILE_WRITE_KAFKA_METADATA")
    boolean getWriteKafkaMetadata();

    @Key("SINK_FILE_WRITER_BLOCK_SIZE")
    int getWriterBlockSize();

    @Key("SINK_FILE_WRITER_PAGE_SIZE")
    int getWriterPageSize();

    @Key("SINK_FILE_ROTATION_DURATION_MILLIS")
    @DefaultValue("-1")
    int getFileRotationDurationMillis();

    @Key("SINK_FILE_ROTATION_MAX_SIZE_BYTES")
    @DefaultValue("-1")
    int getFileRationMaxSizeBytes();

    @Key("SINK_FILE_TIME_PARTITIONING_FIELD_NAME")
    String getTimePartitioningFieldName();

    @Key("SINK_FILE_TIME_PARTITIONING_DATE_PATTERN")
    @DefaultValue("YYYY-MM-dd")
    String getTimePartitioningDatePattern();

    @Key("SINK_FILE_TIME_PARTITIONING_TIME_ZONE")
    @DefaultValue("UTC")
    String getTimePartitioningTimeZone();

    @Key("SINK_FILE_TIME_PARTITIONING_DATE_PREFIX")
    @DefaultValue("dt=")
    String getTimePartitioningDatePrefix();
}
