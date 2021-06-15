package io.odpf.firehose.sink.objectstorage.writer.remote.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.odpf.firehose.sink.objectstorage.writer.remote.ObjectStorage;
import io.odpf.firehose.sink.objectstorage.writer.remote.ObjectStorageUploadFailedException;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@AllArgsConstructor
public class GCSObjectStorage implements ObjectStorage {

    private final GCSWriterConfig gcsWriterConfig;
    @Override
    public void store(String localPath) {
        String objectName = gcsWriterConfig.getLocalBasePath().relativize(Paths.get(localPath)).toString();
        BlobId blobId = BlobId.of(gcsWriterConfig.getGcsBucketName(), objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        try {
            Storage storage = StorageOptions.newBuilder().setProjectId(gcsWriterConfig.getGcsProjectId()).build().getService();
            storage.create(blobInfo, Files.readAllBytes(Paths.get(localPath)));
        } catch (IOException e) {
            e.printStackTrace();
            throw new ObjectStorageUploadFailedException(e);
        }
    }
}
