package io.odpf.firehose.sink.objectstorage.writer.remote;

import io.odpf.firehose.config.ObjectStorageSinkConfig;
import io.odpf.firehose.sink.objectstorage.writer.remote.gcs.GCSObjectStorage;
import io.odpf.firehose.sink.objectstorage.writer.remote.gcs.GCSWriterConfig;

import java.nio.file.Paths;

public class ObjectStorageFactory {

    public static ObjectStorage createObjectStorage(ObjectStorageSinkConfig sinkConfig) {
        switch (sinkConfig.getObjectStorageType()) {
            case "GCS":
                return new GCSObjectStorage(new GCSWriterConfig(
                        Paths.get(sinkConfig.getLocalDirectory()),
                        sinkConfig.getObjectStorageBucketName(),
                        sinkConfig.getStorageGcloudProjectID()));
            default:
                throw new IllegalArgumentException("Storage type " + sinkConfig.getObjectStorageType() + " is not supported");
        }
    }
}
