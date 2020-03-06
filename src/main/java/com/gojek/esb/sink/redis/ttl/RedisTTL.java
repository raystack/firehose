package com.gojek.esb.sink.redis.ttl;

import redis.clients.jedis.Pipeline;

public interface RedisTTL {
    void setTTL(Pipeline jedisPipelined, String key);
}
