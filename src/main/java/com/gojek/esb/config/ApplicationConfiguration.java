package com.gojek.esb.config;

import org.aeonbits.owner.Config;

public interface ApplicationConfiguration extends Config {
    @Key("KAFKA_ADDRESS")
    String getKafkaAddress();

    @Key("CONSUMER_GROUP_ID")
    String getConsumerGroupId();

    @Key("KAFKA_TOPIC")
    String getKafkaTopic();

    @DefaultValue("30000")
    @Key("MAXIMUM_BACKOFF_TIME_MS")
    Integer getMaximumBackOffTimeInMs();
}
