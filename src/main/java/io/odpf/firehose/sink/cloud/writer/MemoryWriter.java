package io.odpf.firehose.sink.cloud.writer;

import io.odpf.firehose.sink.cloud.message.Record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MemoryWriter implements LocalFileWriter {

    private List<Record> records = new ArrayList<>();
    private long createdTimestampMillis;

    public MemoryWriter(long createdTimestampMillis) {
        this.createdTimestampMillis = createdTimestampMillis;
    }

    @Override
    public long currentSize() {
        return records.size();
    }

    @Override
    public void write(Record record) throws IOException {
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
