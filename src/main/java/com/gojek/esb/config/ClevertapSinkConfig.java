package com.gojek.esb.config;

public interface ClevertapSinkConfig extends HTTPSinkConfig {

    @Key("CLEVERTAP_SINK_EVENT_NAME")
    String eventName();

    @Key("CLEVERTAP_SINK_EVENT_TYPE")
    String eventType();

    @Key("CLEVERTAP_SINK_EVENT_TIMESTAMP_INDEX")
    int eventTimestampIndex();

    @Key("CLEVERTAP_SINK_USER_ID_INDEX")
    int useridIndex();

    @Override
    @Key("HTTP_HEADERS")
    @DefaultValue("content-type:application/json")
    String getHTTPHeaders();
}
