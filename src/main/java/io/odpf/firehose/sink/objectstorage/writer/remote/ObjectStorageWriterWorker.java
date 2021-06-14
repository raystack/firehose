package io.odpf.firehose.sink.objectstorage.writer.remote;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@AllArgsConstructor
public class ObjectStorageWriterWorker implements Runnable {

    private final ObjectStorageWriterConfig objectStorageWriterConfig;
    private final String fullPath;

    @Override
    public void run() {
        String objectName = objectStorageWriterConfig.getLocalBasePath().relativize(Paths.get(fullPath)).toString();
        BlobId blobId = BlobId.of(objectStorageWriterConfig.getGcsBucketName(), objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        try {
            Storage storage = StorageOptions.newBuilder().setProjectId(objectStorageWriterConfig.getGcsProjectId()).build().getService();
            storage.create(blobInfo, Files.readAllBytes(Paths.get(fullPath)));
            System.out.println("Pushed to remote " + fullPath + " to " + objectName);
        } catch (IOException e) {
            e.printStackTrace();
            throw new ObjectStorageUploadFailedException(e);
        }

    }
}
