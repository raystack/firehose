package io.odpf.firehose.sink.objectstorage.writer.remote;

public class ObjectStorageFailedException extends RuntimeException {

    public ObjectStorageFailedException(Throwable e) {
        super(e);
    }

}
