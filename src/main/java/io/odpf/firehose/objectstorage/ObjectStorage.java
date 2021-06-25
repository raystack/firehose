package io.odpf.firehose.objectstorage;

import java.io.IOException;

public interface ObjectStorage {
    void store(String localPath);

    void store(String objectName, byte[] content) throws IOException;
}
