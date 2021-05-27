package io.odpf.firehose.sink.file.writer;

import io.odpf.firehose.sink.file.writer.path.PathBuilder;
import io.odpf.firehose.sink.file.message.Record;

import java.io.Closeable;
import java.io.IOException;

public interface FileWriter extends Closeable {
    void open(PathBuilder path) throws IOException;
    void write(Record record) throws IOException;
    long getDataSize();
}
