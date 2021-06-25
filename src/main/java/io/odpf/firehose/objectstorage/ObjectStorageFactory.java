package io.odpf.firehose.objectstorage;

import io.odpf.firehose.objectstorage.gcs.GCSConfig;
import io.odpf.firehose.objectstorage.gcs.GCSObjectStorage;

import java.io.IOException;

public class ObjectStorageFactory {

    public static ObjectStorage createObjectStorage(ObjectStorageType storageType, Object config) {
        if (storageType == ObjectStorageType.GCS) {
            try {
                GCSConfig gcsConfig = (GCSConfig) config;
                return new GCSObjectStorage(gcsConfig);
            } catch (IOException e) {
                throw new IllegalArgumentException("Exception while creating GCS Storage", e);
            }
        }
        throw new IllegalArgumentException("Object Storage Type " + storageType + " is not supported");
    }
}
