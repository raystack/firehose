package io.odpf.firehose.objectstorage.gcs;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import io.odpf.firehose.config.GCSConfig;
import io.odpf.firehose.objectstorage.ObjectStorage;
import io.odpf.firehose.objectstorage.ObjectStorageException;
import io.odpf.firehose.objectstorage.gcs.error.GCSErrorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;

public class GCSObjectStorage implements ObjectStorage {
    private static final Logger LOGGER = LoggerFactory.getLogger(GCSObjectStorage.class);
    private final GCSConfig gcsConfig;
    private final Storage storage;

    public GCSObjectStorage(GCSConfig gcsConfig) throws IOException {
        this.gcsConfig = gcsConfig;
        RetrySettings retrySettings = RetrySettings.newBuilder()
                .setMaxAttempts(gcsConfig.getGCSRetryMaxAttempts())
                .setInitialRetryDelay(Duration.ofMillis(gcsConfig.getGCSRetryInitialDelayMS()))
                .setMaxRetryDelay(Duration.ofMillis(gcsConfig.getGCSRetryMaxDelayMS()))
                .setRetryDelayMultiplier(gcsConfig.getGCSRetryDelayMultiplier())
                .setTotalTimeout(Duration.ofMillis(gcsConfig.getGCSRetryTotalTimeoutMS()))
                .setInitialRpcTimeout(Duration.ofMillis(gcsConfig.getGCSRetryInitialRPCTimeoutMS()))
                .setRpcTimeoutMultiplier(gcsConfig.getGCSRetryRPCTimeoutMultiplier())
                .setMaxRpcTimeout(Duration.ofMillis(gcsConfig.getGCSRetryRPCMaxTimeoutMS()))
                .build();
        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(gcsConfig.getGCSCredentialPath()));
        this.storage = StorageOptions.newBuilder()
                .setProjectId(gcsConfig.getGCloudProjectID())
                .setCredentials(credentials)
                .setRetrySettings(retrySettings)
                .build().getService();
        logRetentionPolicy();
    }

    private void logRetentionPolicy() {
        String bucketName = gcsConfig.getGCSBucketName();
        Bucket bucket = storage.get(bucketName, Storage.BucketGetOption.fields(Storage.BucketField.RETENTION_POLICY));
        LOGGER.info("Retention Policy for " + bucketName);
        LOGGER.info("Retention Period: " + bucket.getRetentionPeriod());
        if (bucket.retentionPolicyIsLocked() != null && bucket.retentionPolicyIsLocked()) {
            LOGGER.info("Retention Policy is locked");
        }
        if (bucket.getRetentionEffectiveTime() != null) {
            LOGGER.info("Effective Time: " + new Date(bucket.getRetentionEffectiveTime()));
        }
    }

    @Override
    public void store(String objectName, String filePath) throws ObjectStorageException {
        try {
            byte[] content = Files.readAllBytes(Paths.get(filePath));
            store(objectName, content);
        } catch (IOException e) {
            LOGGER.error("Failed to read local file " + filePath);
            throw new ObjectStorageException("file_io_error", "File Read failed", e);
        }
    }

    @Override
    public void store(String objectName, byte[] content) throws ObjectStorageException {
        BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(gcsConfig.getGCSBucketName(), objectName)).build();
        try {
            storage.create(blobInfo, content);
            LOGGER.info("Created object in GCS " + blobInfo.getBucket() + "/" + blobInfo.getName());
        } catch (StorageException e) {
            LOGGER.error("Failed to create object in GCS " + blobInfo.getBucket() + "/" + blobInfo.getName());
            String gcsErrorType = GCSErrorType.valueOfCode(e.getCode()).name();
            throw new ObjectStorageException(gcsErrorType, "GCS Upload failed", e);
        }
    }
}
