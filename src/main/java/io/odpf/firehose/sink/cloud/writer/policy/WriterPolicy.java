package io.odpf.firehose.sink.cloud.writer.policy;

import io.odpf.firehose.sink.cloud.writer.LocalFileWriter;

public interface WriterPolicy {
    boolean shouldRotate(LocalFileWriter writer);
}
