package com.gojek.esb.sink.redis.ttl;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

public class NoRedisTTL implements RedisTTL {
    @Override
    public void setTTL(Pipeline jedisPipelined, String key) {
    }

    @Override
    public void setTTL(JedisCluster jedisCluster, String key) {

    }
}
