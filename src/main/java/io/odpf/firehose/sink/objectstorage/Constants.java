package io.odpf.firehose.sink.objectstorage;

public class Constants {
    public enum ObjectStorageType {
        GCS,
        S3,
        HDFS
    }

    public enum WriterType {
        PARQUET,
    }

    public enum PartitioningType {
        NONE,
        DAY,
        HOUR
    }
}
