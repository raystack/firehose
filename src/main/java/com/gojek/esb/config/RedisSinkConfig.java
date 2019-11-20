package com.gojek.esb.config;

public interface RedisSinkConfig extends AppConfig {

    @Key("REDIS_HOST")
    String getRedisHost();

    @Key("REDIS_PORT")
    @DefaultValue("6379")
    Integer getRedisPort();

    @Key("REDIS_KEY_PATTERN")
    String getRedisKeyPattern();

    @Key("REDIS_KEY_VARIABLES")
    String getRedisKeyVariables();
}
