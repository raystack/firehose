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
import java.util.HashMap;
import java.util.Map;

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
        this.bucketName=s3Config.getS3BucketName();

    }

    private void checkBucket(String bucketName) {
        try {
            final HeadBucketRequest request = HeadBucketRequest.builder().bucket(bucketName).build();

            this.s3Client.headBucket(request);
            LOGGER.info("Bucket found "+bucketName);
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
            System.out.println("upload started");
            byte[] content = Files.readAllBytes(Paths.get(filePath));
            store(objectName, content);
        } catch (IOException e) {
            LOGGER.error("Failed to read local file {}", filePath);
            throw new BlobStorageException("file_io_error", "File Read failed", e);
        }

    }

    @Override
    public void store( String objectName, byte[] content) throws BlobStorageException {
        try {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("x-amz-meta-myVal", "test");
            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(this.bucketName)
                    .key(objectName)
                    .build();
            PutObjectResponse response = this.s3Client.putObject(putOb,
                    RequestBody.fromBytes(content));
            System.out.println(response.eTag());
        }
        catch (SdkServiceException ase) {
            LOGGER.error("Caught an AmazonServiceException, which " + "means your request made it "
                    + "to Amazon S3, but was rejected with an error response" + " for some reason.", ase);
            LOGGER.info("Error Message:    " + ase.getMessage());
            LOGGER.info("Key:       " + objectName);
            throw ase;
        } catch (SdkClientException ace) {
            LOGGER.error("Caught an AmazonClientException, which " + "means the client encountered "
                    + "an internal error while trying to " + "communicate with S3, "
                    + "such as not being able to access the network.", ace);
            LOGGER.error("Error Message: {}, {}", objectName, ace.getMessage());
            throw ace;
        }

    }
}
