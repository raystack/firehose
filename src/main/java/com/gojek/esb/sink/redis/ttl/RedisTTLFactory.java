package com.gojek.esb.sink.redis.ttl;

import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.exception.EglcConfigurationException;

public class RedisTTLFactory {

    public static RedisTTL getTTl(RedisSinkConfig redisSinkConfig) {
        long redisTTLValue = redisSinkConfig.getRedisTTLValue();
        if (redisTTLValue < 0) {
            throw new EglcConfigurationException("Provide a positive TTL value");
        }
        switch (redisSinkConfig.getRedisTTLType()) {
            case EXACT_TIME:
                return new ExactTimeTTL(redisTTLValue);
            case DURATION:
                return new DurationTTL((int) redisTTLValue);
            default:
                return new NoRedisTTL();
        }
    }
}
