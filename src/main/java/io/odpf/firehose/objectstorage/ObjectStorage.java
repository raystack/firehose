package io.odpf.firehose.objectstorage;


public interface ObjectStorage {
    void store(String objectName, String filePath) throws ObjectStorageException;

    void store(String objectName, byte[] content) throws ObjectStorageException;
}
