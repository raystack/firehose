package io.odpf.firehose.sink.objectstorage.writer.remote;

public interface ObjectStorage {
    void store(String localPath);
}
