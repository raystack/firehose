package com.gojek.esb.sink.redis.dataentry;

import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.redis.ttl.RedisTTL;
import lombok.AllArgsConstructor;
import lombok.Getter;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

/**
 * Class for Redis Hash set entry.
 */
@AllArgsConstructor
@Getter
public class RedisListEntry implements RedisDataEntry {
    private String key;
    private String value;
    private Instrumentation instrumentation;

    @Override
    public void pushMessage(Pipeline jedisPipelined, RedisTTL redisTTL) {
        getInstrumentation().logDebug("key: {}, value: {}", getKey(), getValue());
        jedisPipelined.lpush(getKey(), getValue());
        redisTTL.setTTL(jedisPipelined, getKey());
    }

    @Override
    public void pushMessage(JedisCluster jedisCluster, RedisTTL redisTTL) {
        getInstrumentation().logDebug("key: {}, value: {}", getKey(), getValue());
        jedisCluster.lpush(getKey(), getValue());
        redisTTL.setTTL(jedisCluster, getKey());
    }
}
