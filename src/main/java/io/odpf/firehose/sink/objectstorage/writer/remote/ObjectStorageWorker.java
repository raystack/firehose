package io.odpf.firehose.sink.objectstorage.writer.remote;

import io.odpf.firehose.objectstorage.ObjectStorage;
import io.odpf.firehose.objectstorage.ObjectStorageException;
import io.odpf.firehose.sink.objectstorage.writer.local.LocalFileMetadata;
import lombok.AllArgsConstructor;

import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;

/**
 * Uploads a local file to object-storage and returns the total time taken.
 */
@AllArgsConstructor
public class ObjectStorageWorker implements Callable<Long> {

    private final ObjectStorage objectStorage;
    private final LocalFileMetadata metadata;

    @Override
    public Long call() throws ObjectStorageException {
        Instant start = Instant.now();
        String objectName = Paths.get(metadata.getBasePath()).relativize(Paths.get(metadata.getFullPath())).toString();
        objectStorage.store(objectName, metadata.getFullPath());
        return Duration.between(start, Instant.now()).toMillis();
    }
}
