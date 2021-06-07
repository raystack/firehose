package io.odpf.firehose.sink.cloud.writer.local.policy;

import io.odpf.firehose.sink.cloud.writer.local.LocalFileWriter;

public interface WriterPolicy {
    boolean shouldRotate(LocalFileWriter writer);
}
