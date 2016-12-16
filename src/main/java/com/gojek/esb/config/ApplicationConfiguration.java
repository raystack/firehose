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

    @Key("DATADOG_PREFIX")
    String getDataDogPrefix();

    @Key("DATADOG_HOST")
    @DefaultValue("localhost")
    String getDataDogHost();

    @Key("DATADOG_PORT")
    @DefaultValue("8125")
    Integer getDataDogPort();

    @Key("DATADOG_TAGS")
    @DefaultValue("")
    String getDataDogTags();
}
