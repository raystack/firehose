package org.raystack.firehose.sink.blob;

public class Constants {
    public enum WriterType {
        PARQUET,
    }

    public enum FilePartitionType {
        NONE,
        DAY,
        HOUR
    }
}
