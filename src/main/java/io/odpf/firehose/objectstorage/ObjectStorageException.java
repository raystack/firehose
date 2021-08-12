package io.odpf.firehose.objectstorage;

/**
 * Should be thrown when there is exception thrown by object storage client.
 */
public class ObjectStorageException extends Exception {
    public ObjectStorageException(Throwable cause) {
        super(cause);
    }
}
