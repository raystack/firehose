package com.gojek.esb.config;

import org.apache.kafka.common.protocol.types.Field;

public interface RedisSinkConfig extends AppConfig {

    @Key("REDIS_HOST")
    String getRedisHost();

    @Key("REDIS_PORT")
    @DefaultValue("6379")
    Integer getRedisPort();

    @Key("REDIS_KEY_TEMPLATE")
    String getRedisKeyTemplate();

    @Key("REDIS_SINK_TYPE")
    String getRedisSinkType();

    @Key("LIST_DATA_PROTO_INDEX")
    String getListDataProtoIndex();
}
