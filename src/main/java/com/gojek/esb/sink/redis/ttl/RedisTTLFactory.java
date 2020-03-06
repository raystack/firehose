package com.gojek.esb.sink.redis.ttl;

import com.gojek.esb.config.RedisSinkConfig;

public class RedisTTLFactory {

    public static RedisTTL getTTl(RedisSinkConfig redisSinkConfig) {
        switch (redisSinkConfig.getRedisTTLType()) {
            case EXACT_TIME:
                return new ExactTimeTTL(redisSinkConfig.getRedisTTLValue());
            case DURATION:
                return new DurationTTL((int) redisSinkConfig.getRedisTTLValue());
            default:
                return new NoRedisTTL();
        }
    }
}
