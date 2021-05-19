package io.odpf.firehose.sink.file;

import java.io.Closeable;
import java.io.IOException;

public interface FileWriter extends Closeable {
    void write(Record record) throws IOException;
}
