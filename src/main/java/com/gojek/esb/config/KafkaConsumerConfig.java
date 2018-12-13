package com.gojek.esb.config;

/**
 * The interface for configurations required to instantiate a consumer.
 */
public interface KafkaConsumerConfig extends AppConfig {
    @Key("ENABLE_ASYNC_COMMIT")
    @DefaultValue("true")
    boolean asyncCommitEnabled();

    @Key("COMMIT_ONLY_CURRENT_PARTITIONS")
    @DefaultValue("true")
    boolean commitOnlyCurrentPartitions();

    @Key("KAFKA_TOPIC")
    String getKafkaTopic();

    @Key("KAFKA_ADDRESS")
    String getKafkaAddress();

    @Key("CONSUMER_GROUP_ID")
    String getConsumerGroupId();

    @Key("KAFKA_CONSUMER_CONFIG_ENABLE_AUTO_COMMIT")
    @DefaultValue("false")
    boolean isAutoCommitEnabled();

    @Key("KAFKA_CONSUMER_CONFIG_METADATA_MAX_AGE_MS")
    @DefaultValue("500")
    int getMetadataMaxAgeInMs();

    @Key("KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS")
    @DefaultValue("500")
    int getMaxPollRecords();

    @Key("KAFKA_CONSUMER_CONFIG_SESSION_TIMEOUT_MS")
    @DefaultValue("10000")
    int getSessionTimeoutInMs();


    @Key("KAFKA_POLL_TIME_OUT")
    @DefaultValue("9223372036854775807")
    Long getPollTimeOut();

    @Key("FILTER_TYPE")
    String getFilterType();

    @Key("FILTER_EXPRESSION")
    String getFilterExpression();

    @Key("FILTER_PROTO_SCHEMA")
    String getFilterProtoSchema();

    @Key("MAX_RETRY_ATTEMPTS")
    @DefaultValue("1")
    Integer getMaximumRetryAttempts();

    @Key("ENABLE_RETRY_QUEUE")
    @DefaultValue("false")
    Boolean getRetryQueueEnabled();

}
