package io.odpf.firehose.sink.cloud.writer.local;

import io.odpf.firehose.sink.cloud.message.Record;
import lombok.Getter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MemoryWriter implements LocalFileWriter {

    @Getter
    private final String fullPath;
    private final List<Record> records = new ArrayList<>();
    private final long createdTimestampMillis;

    public MemoryWriter(long createdTimestampMillis, String fullPath) {
        this.createdTimestampMillis = createdTimestampMillis;
        this.fullPath = fullPath;
    }

    @Override
    public long currentSize() {
        return records.size();
    }

    @Override
    public void write(Record record) throws IOException {
        if (records.size() > 2) {
            throw new IOException("Disk full");
        }
        records.add(record);
    }

    @Override
    public long getCreatedTimestampMillis() {
        return this.createdTimestampMillis;
    }

    @Override
    public void close() throws IOException {
    }
}
