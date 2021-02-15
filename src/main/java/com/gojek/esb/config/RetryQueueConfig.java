package com.gojek.esb.config;

public interface RetryQueueConfig extends AppConfig {

    @Key("retry.queue.kafka.acks")
    @DefaultValue("all")
    String getRetryQueueKafkaAcks();

    @Key("retry.queue.kafka.retries")
    @DefaultValue("2147483647")
    String getRetryQueueKafkaRetries();

    @Key("retry.queue.kafka.batch.size")
    @DefaultValue("16384")
    String getRetryQueueKafkaBatchSize();

    @Key("retry.queue.kafka.linger.ms")
    @DefaultValue("0")
    String getRetryQueueKafkaLingerMs();

    @Key("retry.queue.kafka.buffer.memory")
    @DefaultValue("33554432")
    String getRetryQueueKafkaBufferMemory();

    @Key("retry.queue.kafka.key.serializer")
    @DefaultValue("org.apache.kafka.common.serialization.ByteArraySerializer")
    String getRetryQueueKafkaKeySerializer();

    @Key("retry.queue.kafka.value.serializer")
    @DefaultValue("org.apache.kafka.common.serialization.ByteArraySerializer")
    String getRetryQueueKafkaValueSerializer();

    @Key("retry.queue.kafka.brokers")
    String getRetryQueueKafkaBrokers();

    @Key("retry.queue.kafka.topic")
    @DefaultValue("firehose-retry-topic")
    String getRetryQueueKafkaTopic();
}
