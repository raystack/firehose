package io.odpf.firehose.sink.objectstorage;

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
