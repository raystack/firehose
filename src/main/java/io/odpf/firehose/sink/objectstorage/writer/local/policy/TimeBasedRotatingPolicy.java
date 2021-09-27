package io.odpf.firehose.sink.objectstorage.writer.local.policy;

import io.odpf.firehose.sink.objectstorage.writer.local.LocalFileMetadata;

public class TimeBasedRotatingPolicy implements WriterPolicy {

    private final long maxRotatingDurationMillis;

    public TimeBasedRotatingPolicy(long maxRotatingDurationMillis) {
        if (maxRotatingDurationMillis <= 0) {
            throw new IllegalArgumentException("The max duration should be a positive integer");
        }
        this.maxRotatingDurationMillis = maxRotatingDurationMillis;
    }

    @Override
    public boolean shouldRotate(LocalFileMetadata metadata) {
        return System.currentTimeMillis() - metadata.getCreatedTimestampMillis() >= maxRotatingDurationMillis;
    }
}
