package io.odpf.firehose.sink.objectstorage.writer.remote;

public class ObjectStorageUploadFailedException extends RuntimeException {

    public ObjectStorageUploadFailedException(Throwable e) {
        super(e);
    }

}
