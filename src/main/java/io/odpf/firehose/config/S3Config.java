package io.odpf.firehose.config;

import org.aeonbits.owner.Config;

public interface S3Config extends Config {
    @Key("${S3_TYPE}_S3_REGION")
    String getS3Region();

    @Key("${S3_TYPE}_S3_BUCKET_NAME")
    String getS3BucketName();

    @Key("${S3_TYPE}_S3_ACCESS_KEY")
    String getS3AccessKey();

    @Key("${S3_TYPE}_S3_SECRET_KEY")
    String getS3SecretKey();

    @Key("${S3_TYPE}_S3_RETRY_MAX_ATTEMPTS")
    @DefaultValue("10")
    Integer getS3RetryMaxAttempts();

    @Key("${S3_TYPE}_S3_BASE_DELAY")
    @DefaultValue("1000")
    Long getS3BaseDelay();

    @Key("${S3_TYPE}_S3_MAX_BACKOFF")
    @DefaultValue("30000")
    Long getS3MaxBackoff();

    @Key("${S3_TYPE}_S3_API_ATTEMPT_TIMEOUT")
    @DefaultValue("10000")
    Long getS3ApiAttemptTimeout();

    @Key("${S3_TYPE}_S3_API_TIMEOUT")
    @DefaultValue("40000")
    Long getS3ApiTimeout();
}
