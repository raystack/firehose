package io.odpf.firehose.sink.file.writer.policy;

import io.odpf.firehose.sink.file.writer.LocalFileWriter;

public class TimeBasedRotatingPolicy implements WriterPolicy {

    private final long maxRotatingDurationMillis;

    public TimeBasedRotatingPolicy(long maxRotatingDurationMillis) {
        this.maxRotatingDurationMillis = maxRotatingDurationMillis;
    }

    @Override
    public boolean shouldRotate(LocalFileWriter writer) {
        return System.currentTimeMillis() - writer.getCreatedTimestampMillis() >= maxRotatingDurationMillis;
    }
}
