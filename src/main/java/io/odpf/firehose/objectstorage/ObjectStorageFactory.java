package io.odpf.firehose.objectstorage;

import io.odpf.firehose.objectstorage.gcs.GCSConfig;
import io.odpf.firehose.objectstorage.gcs.GCSObjectStorage;

import java.io.IOException;
import java.util.Properties;

public class ObjectStorageFactory {

    public static ObjectStorage createObjectStorage(ObjectStorageType storageType, Properties config) {
        if (storageType == ObjectStorageType.GCS) {
            try {
                GCSConfig gcsConfig = GCSConfig.create(config);
                return new GCSObjectStorage(gcsConfig);
            } catch (IOException e) {
                throw new IllegalArgumentException("Exception while creating GCS Storage", e);
            }
        }
        throw new IllegalArgumentException("Object Storage Type " + storageType + " is not supported");
    }
}
