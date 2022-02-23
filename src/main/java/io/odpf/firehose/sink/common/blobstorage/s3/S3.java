package io.odpf.firehose.sink.common.blobstorage.s3;


import io.odpf.firehose.config.S3Config;
import io.odpf.firehose.sink.common.blobstorage.BlobStorage;
import io.odpf.firehose.sink.common.blobstorage.BlobStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class S3 implements BlobStorage {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3.class);
    private final S3Client s3Client;
    private final String bucketName;

    public S3(S3Config s3Config) {
        RetryPolicy retryPolicy = RetryPolicy.builder()
                .numRetries(s3Config.getS3RetryMaxAttempts())
                .build();
        ClientOverrideConfiguration overrideConfiguration = ClientOverrideConfiguration.builder()
                .retryPolicy(retryPolicy).build();

        this.s3Client = S3Client.builder()
                .region(Region.of(s3Config.getS3Region()))
                .overrideConfiguration(overrideConfiguration)
                .build();
        checkBucket(s3Config.getS3BucketName());
        this.bucketName = s3Config.getS3BucketName();

    }

    private void checkBucket(String bucketName) {
        try {
            final HeadBucketRequest request = HeadBucketRequest.builder().bucket(bucketName).build();

            this.s3Client.headBucket(request);
            LOGGER.info("Bucket found " + bucketName);
        } catch (NoSuchBucketException ex) {
            LOGGER.error("Bucket not found " + bucketName);
            throw new IllegalArgumentException("S3 Bucket not found " + bucketName + "\n" + ex);

        } catch (S3Exception ex) {
            LOGGER.error("Cannot check access " + bucketName);
            throw new IllegalArgumentException("S3 Bucket not found " + bucketName + "\n" + ex);
        } catch (Exception ex) {
            LOGGER.error("Cannot check access", ex);
            throw ex;
        }
    }

    @Override
    public void store(String objectName, String filePath) throws BlobStorageException {
        try {
            LOGGER.info("upload started");
            byte[] content = Files.readAllBytes(Paths.get(filePath));
            store(objectName, content);
        } catch (IOException e) {
            LOGGER.error("Failed to read local file {}", filePath);
            throw new BlobStorageException("file_io_error", "File Read failed", e);
        }

    }

    @Override
    public void store(String objectName, byte[] content) throws BlobStorageException {
        try {
            PutObjectRequest putObject = PutObjectRequest.builder()
                    .bucket(this.bucketName)
                    .key(objectName)
                    .build();
            this.s3Client.putObject(putObject, RequestBody.fromBytes(content));
            LOGGER.info("Created object in GCS {}", objectName);
        } catch (SdkServiceException | SdkClientException ase) {
            LOGGER.error("Failed to create object in GCS {}", objectName);
            throw new BlobStorageException(ase.getMessage(),ase.getMessage(),ase);
        }

    }
}
