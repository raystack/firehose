package com.gojek.esb.config;

/**
 * The interface for configurations required to instantiate a consumer.
 */
public interface KafkaConsumerConfig extends AppConfig {
    @Key("source.kafka.async.commit.enable")
    @DefaultValue("true")
    boolean isSourceKafkaAsyncCommitEnable();

    @Key("source.kafka.commit.only.current.partitions.enable")
    @DefaultValue("true")
    boolean isSourceKafkaCommitOnlyCurrentPartitionsEnable();

    @Key("source.kafka.topic")
    String getSourceKafkaTopic();

    @Key("source.kafka.brokers")
    String getSourceKafkaBrokers();

    @Key("source.kafka.consumer.group.id")
    String getSourceKafkaConsumerGroupId();

    @Key("source.kafka.consumer.config.auto.commit.enable")
    @DefaultValue("false")
    boolean isSourceKafkaConsumerConfigAutoCommitEnable();

    @Key("source.kafka.consumer.config.metadata.max.age.ms")
    @DefaultValue("500")
    int getSourceKafkaConsumerConfigMetadataMaxAgeMs();

    @Key("source.kafka.consumer.config.max.poll.records")
    @DefaultValue("500")
    int getSourceKafkaConsumerConfigMaxPollRecords();

    @Key("source.kafka.consumer.config.session.timeout.ms")
    @DefaultValue("10000")
    int getSourceKafkaConsumerConfigSessionTimeoutMs();

    @Key("source.kafka.poll.timeout.ms")
    @DefaultValue("9223372036854775807")
    Long getSourceKafkaPollTimeoutMs();

}
