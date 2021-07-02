package io.odpf.firehose.objectstorage;

import io.odpf.firehose.objectstorage.gcs.GCSConfig;
import io.odpf.firehose.objectstorage.gcs.GCSObjectStorage;

import java.io.IOException;
import java.util.Map;

public class ObjectStorageFactory {

    public static ObjectStorage createObjectStorage(ObjectStorageType storageType, Map<String, Object> properties) {
        if (storageType == ObjectStorageType.GCS) {
            try {
                GCSConfig gcsConfig = GCSConfig.create(properties);
                return new GCSObjectStorage(gcsConfig);
            } catch (IOException e) {
                throw new IllegalArgumentException("Exception while creating GCS Storage", e);
            }
        }
        throw new IllegalArgumentException("Object Storage Type " + storageType + " is not supported");
    }
}
