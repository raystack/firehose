package com.gojek.esb.sink.redis.ttl;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

public interface RedisTtl {
    void setTtl(Pipeline jedisPipelined, String key);

    void setTtl(JedisCluster jedisCluster, String key);
}
