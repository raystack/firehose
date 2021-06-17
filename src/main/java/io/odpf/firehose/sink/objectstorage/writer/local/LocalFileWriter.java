package io.odpf.firehose.sink.objectstorage.writer.local;

import io.odpf.firehose.sink.objectstorage.message.Record;

import java.io.Closeable;
import java.io.IOException;

public interface LocalFileWriter extends Closeable {
    long currentSize();

    void write(Record record) throws IOException;

    long getCreatedTimestampMillis();

    String getFullPath();
}
