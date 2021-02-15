package com.gojek.esb.sink.redis.ttl;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

public class NoRedisTtl implements RedisTtl {
    @Override
    public void setTtl(Pipeline jedisPipelined, String key) {
    }

    @Override
    public void setTtl(JedisCluster jedisCluster, String key) {

    }
}
