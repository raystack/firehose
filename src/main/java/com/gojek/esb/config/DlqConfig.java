package com.gojek.esb.config;

public interface DlqConfig extends AppConfig {

    @Key("dlq.attempts.to.trigger")
    @DefaultValue("1")
    Integer getDlqAttemptsToTrigger();

    @Key("dlq.kafka.acks")
    @DefaultValue("all")
    String getDlqKafkaAcks();

    @Key("dlq.kafka.retries")
    @DefaultValue("2147483647")
    String getDlqKafkaRetries();

    @Key("dlq.kafka.batch.size")
    @DefaultValue("16384")
    String getDlqKafkaBatchSize();

    @Key("dlq.kafka.linger.ms")
    @DefaultValue("0")
    String getDlqKafkaLingerMs();

    @Key("dlq.kafka.buffer.memory")
    @DefaultValue("33554432")
    String getDlqKafkaBufferMemory();

    @Key("dlq.kafka.key.serializer")
    @DefaultValue("org.apache.kafka.common.serialization.ByteArraySerializer")
    String getDlqKafkaKeySerializer();

    @Key("dlq.kafka.value.serializer")
    @DefaultValue("org.apache.kafka.common.serialization.ByteArraySerializer")
    String getDlqKafkaValueSerializer();

    @Key("dlq.kafka.brokers")
    String getDlqKafkaBrokers();

    @Key("dlq.kafka.topic")
    @DefaultValue("firehose-retry-topic")
    String getDlqKafkaTopic();
}
