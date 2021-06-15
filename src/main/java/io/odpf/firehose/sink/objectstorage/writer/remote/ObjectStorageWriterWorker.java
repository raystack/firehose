package io.odpf.firehose.sink.objectstorage.writer.remote;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ObjectStorageWriterWorker implements Runnable {

    private final ObjectStorage storage;
    private final String fullPath;

    @Override
    public void run() {
        storage.store(fullPath);
    }
}
