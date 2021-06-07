package io.odpf.firehose.sink.cloud;

public class Constants {
    enum CloudSinkType {
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
