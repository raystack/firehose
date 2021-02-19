package com.gojek.esb.sink.redis.ttl;

import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.exception.EglcConfigurationException;

public class RedisTTLFactory {

    public static RedisTtl getTTl(RedisSinkConfig redisSinkConfig) {
        long redisTTLValue = redisSinkConfig.getSinkRedisTtlValue();
        if (redisTTLValue < 0) {
            throw new EglcConfigurationException("Provide a positive TTL value");
        }
        switch (redisSinkConfig.getSinkRedisTtlType()) {
            case EXACT_TIME:
                return new ExactTimeTtl(redisTTLValue);
            case DURATION:
                return new DurationTtl((int) redisTTLValue);
            default:
                return new NoRedisTtl();
        }
    }
}
