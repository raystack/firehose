package io.odpf.firehose.objectstorage.gcs;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import io.odpf.firehose.config.GCSConfig;
import io.odpf.firehose.objectstorage.ObjectStorage;
import io.odpf.firehose.objectstorage.ObjectStorageException;
import io.odpf.firehose.objectstorage.gcs.error.GCSErrorType;
import io.odpf.firehose.sink.objectstorage.writer.remote.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;
import org.threeten.bp.temporal.ChronoUnit;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class GCSObjectStorage implements ObjectStorage {
    private static final Logger LOGGER = LoggerFactory.getLogger(GCSObjectStorage.class);
    public static final long INITIAL_RETRY_DELAY = 1000L;
    public static final long MAX_RETRY_DELAY = 32_000L;
    public static final double RETRY_DELAY_MULTIPLIER = 2.0;
    public static final long INITIAL_RPC_TIMEOUT = 5_000L;
    public static final double RPC_TIMEOUT_MULTIPLIER = 1.0;
    public static final long MAX_RPC_TIMEOUT = 5_000L;
    private final GCSConfig gcsConfig;
    private final Storage storage;

    public GCSObjectStorage(GCSConfig gcsConfig) throws IOException {
        this.gcsConfig = gcsConfig;
        // only MaxAttempts and TotalTimeout configuration is exposed to the users
        // its enough for the users to set the duration of retry or retry attempts
        // retry effort is stopped when one of the condition are met
        // assume in one call have 1 rpc call then, total time that will be took when retry calculated roughly
        // 5 * attempt + initial delay (1) * (2 ^ attempt)
        // further on the maximum attempts and total timeout config here {@link com.google.api.gax.retrying.RetrySettings}
        RetrySettings retrySettings = RetrySettings.newBuilder()
                //maximum attempts of API call
                .setMaxAttempts(gcsConfig.getGCSRetryMaxAttempts())
                //The timeout for all calls (first call + all retries),
                //calculated since the first time of call until last attempt
                //further {@link com.google.api.gax.retrying.ExponentialRetryAlgorithm#shouldRetry(TimedAttemptSettings)}
                .setTotalTimeout(Duration.of(gcsConfig.getGCSRetryTimeoutDurationMillis(), ChronoUnit.MILLIS))
                //first delay before delay increased based on delay multiplier calculation
                .setInitialRetryDelay(Duration.ofMillis(INITIAL_RETRY_DELAY))
                //maximum delay after delay multiplied each try attempt
                .setMaxRetryDelay(Duration.ofMillis(MAX_RETRY_DELAY))
                //produce delay on API call, multiplier * num of attempt
                //we want to increase the delay when retrying
                .setRetryDelayMultiplier(RETRY_DELAY_MULTIPLIER)

                //RPC call config, one API call might multiple rpc call
                //initial RPC timeout for waiting before retry
                .setInitialRpcTimeout(Duration.ofMillis(INITIAL_RPC_TIMEOUT))
                //multiplier, increase timeout duration for each retry
                //we dont need to increase delay here
                .setRpcTimeoutMultiplier(RPC_TIMEOUT_MULTIPLIER)
                //maximum timeout duration after multiplied
                .setMaxRpcTimeout(Duration.ofMillis(MAX_RPC_TIMEOUT))
                .build();
        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(gcsConfig.getGCSCredentialPath()));
        this.storage = StorageOptions.newBuilder()
                .setProjectId(gcsConfig.getGCloudProjectID())
                .setCredentials(credentials)
                .setRetrySettings(retrySettings)
                .build().getService();
    }

    @Override
    public void store(String localPath) throws ObjectStorageException {
        String objectName = Paths.get(gcsConfig.getGCSLocalDirectory()).relativize(Paths.get(localPath)).toString();
        byte[] content;
        try {
            content = Files.readAllBytes(Paths.get(localPath));
        } catch (IOException e) {
            LOGGER.error("Failed to read local file " + localPath);
            throw new ObjectStorageException(Constants.FILE_IO_ERROR, "File Read failed", e);
        }
        store(objectName, content);
    }

    @Override
    public void store(String objectName, byte[] content) throws ObjectStorageException {
        BlobId blobId = BlobId.of(gcsConfig.getGCSBucketName(), objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
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
