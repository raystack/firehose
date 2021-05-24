package io.odpf.firehose.sink.file;

import java.nio.file.Path;

/**
 * PathFactory is creator of path factory
 */
public interface PathFactory {
    // TODO: 21/05/21 path factory should modify PathBuilder instead return new path
    Path create(Record record);
}