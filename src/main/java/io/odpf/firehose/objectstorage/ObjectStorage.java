package io.odpf.firehose.objectstorage;

import java.io.IOException;

public interface ObjectStorage {
    void store(String localPath) throws IOException, ObjectStorageException;

    void store(String objectName, byte[] content) throws ObjectStorageException;
}
