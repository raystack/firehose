package com.gojek.esb.config;

import org.aeonbits.owner.Config;

public interface ApplicationConfiguration extends Config {
    @Key("KAFKA_ADDRESS")
    String getKafkaAddress();

    @Key("CONSUMER_GROUP_ID")
    String getConsumerGroupId();

    @Key("KAFKA_TOPIC")
    String getKafkaTopic();

    @Key("SERVICE_URL")
    String getServiceURL();

    @Key("HTTP_HEADERS")
    String getHTTPHeaders();
}
