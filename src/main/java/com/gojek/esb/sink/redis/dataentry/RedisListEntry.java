package com.gojek.esb.sink.redis.dataentry;

import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.redis.ttl.RedisTtl;
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
    public void pushMessage(Pipeline jedisPipelined, RedisTtl redisTTL) {
        getInstrumentation().logDebug("key: {}, value: {}", getKey(), getValue());
        jedisPipelined.lpush(getKey(), getValue());
        redisTTL.setTtl(jedisPipelined, getKey());
    }

    @Override
    public void pushMessage(JedisCluster jedisCluster, RedisTtl redisTTL) {
        getInstrumentation().logDebug("key: {}, value: {}", getKey(), getValue());
        jedisCluster.lpush(getKey(), getValue());
        redisTTL.setTtl(jedisCluster, getKey());
    }
}
