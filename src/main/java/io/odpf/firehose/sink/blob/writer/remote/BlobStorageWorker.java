package io.odpf.firehose.sink.blob.writer.remote;

import io.odpf.firehose.blobstorage.BlobStorage;
import io.odpf.firehose.blobstorage.BlobStorageException;
import io.odpf.firehose.sink.blob.writer.local.LocalFileMetadata;
import lombok.AllArgsConstructor;

import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;

/**
 * Uploads a local file to object-storage and returns the total time taken.
 */
@AllArgsConstructor
public class BlobStorageWorker implements Callable<Long> {

    private final BlobStorage blobStorage;
    private final LocalFileMetadata metadata;

    @Override
    public Long call() throws BlobStorageException {
        Instant start = Instant.now();
        String objectName = Paths.get(metadata.getBasePath()).relativize(Paths.get(metadata.getFullPath())).toString();
        blobStorage.store(objectName, metadata.getFullPath());
        return Duration.between(start, Instant.now()).toMillis();
    }
}
