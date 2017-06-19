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
    @DefaultValue("")
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

    @Key("ENABLE_AUDIT")
    @DefaultValue("false")
    Boolean isAuditEnabled();

    @Key("AUDIT_SERVICE_URL")
    String getAuditServiceUrl();

    @Key("SINK")
    @ConverterClass(SinkConverter.class)
    SinkType getSinkType();

    @Key("NUMBER_OF_CONSUMERS_THREADS")
    @DefaultValue("1")
    Integer noOfConsumerThreads();

    @Key("DELAY_TO_CLEAN_UP_CONSUMER_THREADS")
    @DefaultValue("2000")
    Integer threadCleanupDelay();

    @Key("SINK_FACTORY_CLASS")
    String sinkFactoryClass();
}
