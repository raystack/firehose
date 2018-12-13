package com.gojek.esb.config;

public interface RetryQueueConfig extends AppConfig {

    @Key("RETRY_QUEUE_KAFKA_ACKS")
    @DefaultValue("all")
    String getRetryQueueKafkaAcks();

    @Key("RETRY_QUEUE_KAFKA_RETRIES")
    @DefaultValue("2147483647")
    String getRetryQueueKafkaRetries();

    @Key("RETRY_QUEUE_KAFKA_BATCH_SIZE")
    @DefaultValue("16384")
    String getRetryQueueKafkaBatchSize();

    @Key("RETRY_QUEUE_KAFKA_LINGER_MS")
    @DefaultValue("0")
    String getRetryQueueKafkaLingerSize();

    @Key("RETRY_QUEUE_KAFKA_BUFFER_MEMORY")
    @DefaultValue("33554432")
    String getRetryQueueKafkaBufferMemory();

    @Key("RETRY_QUEUE_KAFKA_KEY_SERIALIZER")
    @DefaultValue("org.apache.kafka.common.serialization.ByteArraySerializer")
    String getRetryQueueKafkaKeySerializer();

    @Key("RETRY_QUEUE_KAFKA_VALUE_SERIALIZER")
    @DefaultValue("org.apache.kafka.common.serialization.ByteArraySerializer")
    String getRetryQueueKafkaValueSerializer();

    @Key("RETRY_QUEUE_KAFKA_BOOTSTRAP_SERVERS")
    String getRetryQueueKafkaBootStrapServers();

    @Key("RETRY_TOPIC")
    @DefaultValue("false")
    String getRetryTopic();
}
