package io.odpf.firehose.sink.file.writer;

import io.odpf.firehose.sink.file.message.Record;

import java.io.Closeable;
import java.io.IOException;

public interface LocalFileWriter extends Closeable {
    long currentSize();
    void write(Record record) throws IOException;

    long getCreatedTimestampMillis();
}
