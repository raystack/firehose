package io.odpf.firehose.sink.redis.ttl;

import lombok.AllArgsConstructor;
import lombok.Getter;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;


@AllArgsConstructor
@Getter
public class DurationTtl implements RedisTtl {
    private int ttlInSeconds;

    @Override
    public void setTtl(Pipeline jedisPipelined, String key) {
        jedisPipelined.expire(key, ttlInSeconds);
    }

    @Override
    public void setTtl(JedisCluster jedisCluster, String key) {
        jedisCluster.expire(key, ttlInSeconds);
    }
}
