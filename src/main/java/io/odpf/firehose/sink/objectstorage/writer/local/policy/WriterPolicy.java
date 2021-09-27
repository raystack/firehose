package io.odpf.firehose.sink.objectstorage.writer.local.policy;

import io.odpf.firehose.sink.objectstorage.writer.local.LocalFileMetadata;

public interface WriterPolicy {
    boolean shouldRotate(LocalFileMetadata metadata);
}
