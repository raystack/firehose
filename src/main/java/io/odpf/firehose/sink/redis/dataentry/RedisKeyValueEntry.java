package io.odpf.firehose.sink.redis.dataentry;

import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.firehose.sink.redis.ttl.RedisTtl;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

@AllArgsConstructor
@Getter
@EqualsAndHashCode(exclude = "firehoseInstrumentation")
public class RedisKeyValueEntry implements RedisDataEntry {

    private String key;
    private String value;
    private FirehoseInstrumentation firehoseInstrumentation;

    @Override
    public void pushMessage(Pipeline jedisPipelined, RedisTtl redisTTL) {
        firehoseInstrumentation.logDebug("key: {}, value: {}", key, value);
        jedisPipelined.set(key, value);
        redisTTL.setTtl(jedisPipelined, key);
    }

    @Override
    public void pushMessage(JedisCluster jedisCluster, RedisTtl redisTTL) {
        firehoseInstrumentation.logDebug("key: {}, value: {}", key, value);
        jedisCluster.set(key, value);
        redisTTL.setTtl(jedisCluster, key);

    }

    @Override
    public String toString() {
        return "RedisKeyValueEntry{"
                +
                "key='"
                + key
                + '\''
                +
                ", value='" + value
                + '\''
                +
                '}';
    }
}
