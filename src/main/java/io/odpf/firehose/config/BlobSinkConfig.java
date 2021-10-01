package io.odpf.firehose.config;

import io.odpf.firehose.config.converter.BlobSinkLocalFileWriterTypeConverter;
import io.odpf.firehose.config.converter.BlobSinkPartitioningTypeConverter;
import io.odpf.firehose.config.converter.BlobStorageTypeConverter;
import io.odpf.firehose.blobstorage.BlobStorageType;
import io.odpf.firehose.sink.blob.Constants;

public interface BlobSinkConfig extends AppConfig {

    @Key("SINK_BLOB_STORAGE_TYPE")
    @DefaultValue("GCS")
    @ConverterClass(BlobStorageTypeConverter.class)
    BlobStorageType getBlobStorageType();

    @Key("SINK_BLOB_LOCAL_DIRECTORY")
    @DefaultValue("/tmp/firehose")
    String getLocalDirectory();

    @Key("SINK_BLOB_LOCAL_FILE_WRITER_TYPE")
    @DefaultValue("parquet")
    @ConverterClass(BlobSinkLocalFileWriterTypeConverter.class)
    Constants.WriterType getFileWriterType();

    @Key("SINK_BLOB_WRITER_PARQUET_BLOCK_SIZE")
    @DefaultValue("134217728")
    int getWriterParquetBlockSize();

    @Key("SINK_BLOB_WRITER_PARQUET_PAGE_SIZE")
    @DefaultValue("1048576")
    int getWriterParquetPageSize();

    @Key("SINK_BLOB_KAFKA_METADATA_COLUMN_NAME")
    @DefaultValue("")
    String getKafkaMetadataColumnName();

    @Key("SINK_BLOB_WRITE_KAFKA_METADATA_ENABLE")
    boolean getWriteKafkaMetadataEnable();

    @Key("SINK_BLOB_FILE_ROTATION_DURATION_MS")
    @DefaultValue("3600000")
    long getFileRotationDurationMS();

    @Key("SINK_BLOB_FILE_ROTATION_MAX_SIZE_BYTES")
    @DefaultValue("268435456")
    long getFileRotationMaxSizeBytes();

    @Key("SINK_BLOB_TIME_PARTITIONING_FIELD_NAME")
    String getTimePartitioningFieldName();

    @Key("SINK_BLOB_TIME_PARTITIONING_TYPE")
    @DefaultValue("day")
    @ConverterClass(BlobSinkPartitioningTypeConverter.class)
    Constants.FilePartitionType getPartitioningType();

    @Key("SINK_BLOB_TIME_PARTITIONING_TIME_ZONE")
    @DefaultValue("UTC")
    String getTimePartitioningTimeZone();

    @Key("SINK_BLOB_TIME_PARTITIONING_DATE_PREFIX")
    @DefaultValue("dt=")
    String getTimePartitioningDatePrefix();

    @Key("SINK_BLOB_TIME_PARTITIONING_HOUR_PREFIX")
    @DefaultValue("hr=")
    String getTimePartitioningHourPrefix();
}
