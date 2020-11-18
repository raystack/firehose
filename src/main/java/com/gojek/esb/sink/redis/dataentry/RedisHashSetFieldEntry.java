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
public class RedisHashSetFieldEntry implements RedisDataEntry {

    private String key;
    private String field;
    private String value;
    private Instrumentation instrumentation;

    @Override
    public void pushMessage(Pipeline jedisPipelined, RedisTTL redisTTL) {
        redisTTL.setTTL(jedisPipelined, getKey());
        getInstrumentation().logDebug("key: {}, field: {}, value: {}", getKey(), getField(), getValue());
        jedisPipelined.hset(getKey(), getField(), getValue());
    }

    @Override
    public void pushMessage(JedisCluster jedisCluster, RedisTTL redisTTL) {
        redisTTL.setTTL(jedisCluster, getKey());
        getInstrumentation().logDebug("key: {}, field: {}, value: {}", getKey(), getField(), getValue());
        jedisCluster.hset(getKey(), getField(), getValue());
    }
}
