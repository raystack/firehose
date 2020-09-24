package com.gojek.esb.config;

import com.gojek.esb.config.converter.RedisSinkTypeConverter;
import com.gojek.esb.config.converter.RedisTTLTypeConverter;
import com.gojek.esb.config.converter.RedisServerTypeConverter;
import com.gojek.esb.config.enums.RedisSinkType;
import com.gojek.esb.config.enums.RedisTTLType;
import com.gojek.esb.config.enums.RedisServerType;

public interface RedisSinkConfig extends AppConfig {
    @Key("REDIS_URLS")
    String getRedisUrls();

    @Key("REDIS_KEY_TEMPLATE")
    String getRedisKeyTemplate();

    @Key("REDIS_SINK_TYPE")
    @DefaultValue("HASHSET")
    @ConverterClass(RedisSinkTypeConverter.class)
    RedisSinkType getRedisSinkType();

    @Key("REDIS_LIST_DATA_PROTO_INDEX")
    String getRedisListDataProtoIndex();

    @Key("REDIS_TTL_TYPE")
    @DefaultValue("DISABLE")
    @ConverterClass(RedisTTLTypeConverter.class)
    RedisTTLType getRedisTTLType();

    @Key("REDIS_TTL_VALUE")
    @DefaultValue("0")
    long getRedisTTLValue();

    @Key("REDIS_SERVER_TYPE")
    @DefaultValue("Standalone")
    @ConverterClass(RedisServerTypeConverter.class)
    RedisServerType getRedisServerType();


}
