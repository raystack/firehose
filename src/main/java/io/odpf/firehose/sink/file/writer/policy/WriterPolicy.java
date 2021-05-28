package io.odpf.firehose.sink.file.writer.policy;

import io.odpf.firehose.sink.file.writer.LocalFileWriter;

public interface WriterPolicy {

    boolean shouldRotate(LocalFileWriter writer);
}
