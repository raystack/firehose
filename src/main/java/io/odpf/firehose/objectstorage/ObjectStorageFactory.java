package io.odpf.firehose.objectstorage;

import io.odpf.firehose.config.GCSConfig;
import io.odpf.firehose.objectstorage.gcs.GCSObjectStorage;
import org.aeonbits.owner.ConfigFactory;

import java.io.IOException;
import java.util.Map;

public class ObjectStorageFactory {

    public static ObjectStorage createObjectStorage(ObjectStorageType storageType, Map<String, String> config) {
        if (storageType == ObjectStorageType.GCS) {
            try {
                GCSConfig gcsConfig = ConfigFactory.create(GCSConfig.class, config);
                GCSObjectStorage gcsObjectStorage = new GCSObjectStorage(gcsConfig);
                return gcsObjectStorage;
            } catch (IOException e) {
                throw new IllegalArgumentException("Exception while creating GCS Storage", e);
            }
        }
        throw new IllegalArgumentException("Object Storage Type " + storageType + " is not supported");
    }
}
