package io.odpf.firehose.sink.file;

import java.nio.file.Path;

/**
 * PathFactory is creator of path factory
 */
public interface PathFactory {
    Path create(Record record);
}