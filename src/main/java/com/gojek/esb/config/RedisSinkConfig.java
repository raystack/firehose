package com.gojek.esb.config;

import com.gojek.esb.config.converter.RedisSinkDataTypeConverter;
import com.gojek.esb.config.converter.RedisSinkTtlTypeConverter;
import com.gojek.esb.config.converter.RedisSinkDeploymentTypeConverter;
import com.gojek.esb.config.enums.RedisSinkDataType;
import com.gojek.esb.config.enums.RedisSinkTtlType;
import com.gojek.esb.config.enums.RedisSinkDeploymentType;

public interface RedisSinkConfig extends AppConfig {
    @Key("sink.redis.urls")
    String getSinkRedisUrls();

    @Key("sink.redis.key.template")
    String getSinkRedisKeyTemplate();

    @Key("sink.redis.data.type")
    @DefaultValue("HASHSET")
    @ConverterClass(RedisSinkDataTypeConverter.class)
    RedisSinkDataType getSinkRedisDataType();

    @Key("sink.redis.list.data.proto.index")
    String getSinkRedisListDataProtoIndex();

    @Key("sink.redis.ttl.type")
    @DefaultValue("DISABLE")
    @ConverterClass(RedisSinkTtlTypeConverter.class)
    RedisSinkTtlType getSinkRedisTtlType();

    @Key("sink.redis.ttl.value")
    @DefaultValue("0")
    long getSinkRedisTtlValue();

    @Key("sink.redis.deployment.type")
    @DefaultValue("Standalone")
    @ConverterClass(RedisSinkDeploymentTypeConverter.class)
    RedisSinkDeploymentType getSinkRedisServerType();


}
