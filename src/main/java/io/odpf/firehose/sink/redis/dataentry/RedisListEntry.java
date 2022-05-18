package io.odpf.firehose.sink.redis.dataentry;

import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.firehose.sink.redis.ttl.RedisTtl;
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
    private FirehoseInstrumentation firehoseInstrumentation;

    @Override
    public void pushMessage(Pipeline jedisPipelined, RedisTtl redisTTL) {
        getFirehoseInstrumentation().logDebug("key: {}, value: {}", getKey(), getValue());
        jedisPipelined.lpush(getKey(), getValue());
        redisTTL.setTtl(jedisPipelined, getKey());
    }

    @Override
    public void pushMessage(JedisCluster jedisCluster, RedisTtl redisTTL) {
        getFirehoseInstrumentation().logDebug("key: {}, value: {}", getKey(), getValue());
        jedisCluster.lpush(getKey(), getValue());
        redisTTL.setTtl(jedisCluster, getKey());
    }
}
