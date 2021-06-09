package io.odpf.firehose.sink.objectstorage.writer.remote;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ObjectStorageWriterWorker implements Runnable {

    private final String projectID;
    private final String bucketName;
    private final String basePath;
    private final String fullPath;

    public ObjectStorageWriterWorker(String projectID, String bucketName, String basePath, String fullPath) {
        this.bucketName = bucketName;
        this.basePath = basePath;
        this.fullPath = fullPath;
        this.projectID = projectID;
    }

    @Override
    public void run() {
        String objectName = Paths.get(basePath).relativize(Paths.get(fullPath)).toString();

        BlobId blobId = BlobId.of(bucketName, objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        try {
            Storage storage = StorageOptions.newBuilder().setProjectId(this.projectID).build().getService();
            storage.create(blobInfo, Files.readAllBytes(Paths.get(fullPath)));
        } catch (IOException e) {
            e.printStackTrace();
            throw new ObjectStorageUploadFailedException(e);
        }

    }
}
