package io.odpf.firehose.sink.objectstorage.writer.remote;

import io.odpf.firehose.config.ObjectStorageSinkConfig;
import io.odpf.firehose.sink.objectstorage.writer.remote.gcs.GCSObjectStorage;
import io.odpf.firehose.sink.objectstorage.writer.remote.gcs.GCSWriterConfig;

import java.io.IOException;
import java.nio.file.Paths;

public class ObjectStorageFactory {

    public static ObjectStorage createObjectStorage(ObjectStorageSinkConfig sinkConfig) {
        switch (sinkConfig.getObjectStorageType()) {
            case "GCS":
                try {
                    return new GCSObjectStorage(new GCSWriterConfig(
                            Paths.get(sinkConfig.getLocalDirectory()),
                            sinkConfig.getGCSBucketName(),
                            sinkConfig.getGCSCredentialPath(),
                            sinkConfig.getGCloudProjectID()));
                } catch (IOException e) {
                    throw new IllegalArgumentException("Exception while creating GCS Storage", e);
                }
            default:
                throw new IllegalArgumentException("Storage type " + sinkConfig.getObjectStorageType() + " is not supported");
        }
    }
}
