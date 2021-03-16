package com.gojek.esb.config;

import com.gojek.esb.config.converter.RedisSinkDataTypeConverter;
import com.gojek.esb.config.converter.RedisSinkTtlTypeConverter;
import com.gojek.esb.config.converter.RedisSinkDeploymentTypeConverter;
import com.gojek.esb.config.enums.RedisSinkDataType;
import com.gojek.esb.config.enums.RedisSinkTtlType;
import com.gojek.esb.config.enums.RedisSinkDeploymentType;

public interface RedisSinkConfig extends AppConfig {
    @Key("SINK_REDIS_URLS")
    String getSinkRedisUrls();

    @Key("SINK_REDIS_KEY_TEMPLATE")
    String getSinkRedisKeyTemplate();

    @Key("SINK_REDIS_DATA_TYPE")
    @DefaultValue("HASHSET")
    @ConverterClass(RedisSinkDataTypeConverter.class)
    RedisSinkDataType getSinkRedisDataType();

    @Key("SINK_REDIS_LIST_DATA_PROTO_INDEX")
    String getSinkRedisListDataProtoIndex();

    @Key("SINK_REDIS_TTL_TYPE")
    @DefaultValue("DISABLE")
    @ConverterClass(RedisSinkTtlTypeConverter.class)
    RedisSinkTtlType getSinkRedisTtlType();

    @Key("SINK_REDIS_TTL_VALUE")
    @DefaultValue("0")
    long getSinkRedisTtlValue();

    @Key("SINK_REDIS_DEPLOYMENT_TYPE")
    @DefaultValue("Standalone")
    @ConverterClass(RedisSinkDeploymentTypeConverter.class)
    RedisSinkDeploymentType getSinkRedisDeploymentType();


}
