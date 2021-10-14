package io.odpf.firehose.sink.blob.writer.local.policy;

import io.odpf.firehose.sink.blob.writer.local.LocalFileMetadata;

public interface WriterPolicy {
    boolean shouldRotate(LocalFileMetadata metadata);
}
