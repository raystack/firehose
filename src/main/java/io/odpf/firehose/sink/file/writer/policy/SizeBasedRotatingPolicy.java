package io.odpf.firehose.sink.file.writer.policy;

import io.odpf.firehose.sink.file.writer.LocalFileWriter;

public class SizeBasedRotatingPolicy implements WriterPolicy {

    private final long maxSize;

    public SizeBasedRotatingPolicy(long maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public boolean shouldRotate(LocalFileWriter writer) {
        return writer.currentSize() >= maxSize;
    }
}
