package com.gojek.esb.config;

import com.gojek.esb.config.converter.RedisSinkTypeConverter;
import com.gojek.esb.config.enums.RedisSinkType;

public interface RedisSinkConfig extends AppConfig {

    @Key("REDIS_HOST")
    String getRedisHost();

    @Key("REDIS_PORT")
    @DefaultValue("6379")
    Integer getRedisPort();

    @Key("REDIS_KEY_TEMPLATE")
    String getRedisKeyTemplate();

    @Key("REDIS_SINK_TYPE")
    @ConverterClass(RedisSinkTypeConverter.class)
    RedisSinkType getRedisSinkType();

    @Key("LIST_DATA_PROTO_INDEX")
    String getListDataProtoIndex();
}
