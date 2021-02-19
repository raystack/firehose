package com.gojek.esb.sink.redis.ttl;

import lombok.AllArgsConstructor;
import lombok.Getter;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;


@AllArgsConstructor
@Getter
public class ExactTimeTtl implements RedisTtl {
    private long unixTime;

    @Override
    public void setTtl(Pipeline jedisPipelined, String key) {
        jedisPipelined.expireAt(key, unixTime);
    }

    @Override
    public void setTtl(JedisCluster jedisCluster, String key) {
        jedisCluster.expireAt(key, unixTime);
    }
}
