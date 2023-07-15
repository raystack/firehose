package org.raystack.firehose.sink.blob.writer.local.policy;

import org.raystack.firehose.sink.blob.writer.local.LocalFileMetadata;

public interface WriterPolicy {
    boolean shouldRotate(LocalFileMetadata metadata);
}
