package io.odpf.firehose.config;

import io.odpf.firehose.config.converter.CloudSinkPartitioningTypeConverter;
import io.odpf.firehose.sink.cloud.Constants;

public interface CloudSinkConfig extends AppConfig {

    @Key("SINK_CLOUD_LOCAL_DIRECTORY")
    String getLocalDirectory();

    @Key("SINK_CLOUD_LOCAL_WRITER_CLASS")
    @DefaultValue("io.odpf.firehose.sink.cloud.writer.local.LocalParquetFileWriter")
    String getLocalFileWriterClass();

    @Key("SINK_CLOUD_KAFKA_METADATA_COLUMN_NAME")
    @DefaultValue("")
    String getKafkaMetadataColumnName();

    @Key("SINK_CLOUD_WRITE_KAFKA_METADATA")
    boolean getWriteKafkaMetadata();

    @Key("SINK_CLOUD_WRITER_BLOCK_SIZE")
    int getWriterBlockSize();

    @Key("SINK_CLOUD_WRITER_PAGE_SIZE")
    int getWriterPageSize();

    @Key("SINK_CLOUD_ROTATION_DURATION_MILLIS")
    @DefaultValue("3600000")
    int getFileRotationDurationMillis();

    @Key("SINK_CLOUD_ROTATION_MAX_SIZE_BYTES")
    @DefaultValue("268435456")
    int getFileRationMaxSizeBytes();

    @Key("SINK_CLOUD_TIME_PARTITIONING_FIELD_NAME")
    String getTimePartitioningFieldName();

    @Key("SINK_CLOUD_TIME_PARTITIONING_DATE_PATTERN")
    @DefaultValue("YYYY-MM-dd")
    String getTimePartitioningDatePattern();

    @Key("SINK_CLOUD_TIME_PARTITIONING_TIME_ZONE")
    @DefaultValue("UTC")
    String getTimePartitioningTimeZone();

    @Key("SINK_CLOUD_TIME_PARTITIONING_DATE_PREFIX")
    @DefaultValue("dt=")
    String getTimePartitioningDatePrefix();

    @Key("SINK_CLOUD_TIME_PARTITIONING_TYPE")
    @DefaultValue("day")
    @ConverterClass(CloudSinkPartitioningTypeConverter.class)
    Constants.PartitioningType getPartitioningType();

    @Key("SINK_CLOUD_TIME_PARTITIONING_HOUR_PREFIX")
    @DefaultValue("hr=")
    String getTimePartitioningHourPrefix();

    @Key("SINK_CLOUD_STORAGE_TYPE")
    @DefaultValue("GCS")
    String getCloudStorageType();
}
