package com.gojek.esb.sink.redis.ttl;

import lombok.AllArgsConstructor;
import lombok.Getter;
import redis.clients.jedis.Pipeline;


@AllArgsConstructor
@Getter
public class DurationTTL implements RedisTTL {
    private int ttlInSeconds;

    @Override
    public void setTTL(Pipeline jedisPipelined, String key) {
        jedisPipelined.expire(key, ttlInSeconds);
    }
}
