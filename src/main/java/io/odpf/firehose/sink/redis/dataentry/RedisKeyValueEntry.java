package io.odpf.firehose.sink.redis.dataentry;

import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.redis.ttl.RedisTtl;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class RedisKeyValueEntry implements RedisDataEntry {

    private String key;
    private String value;
    @EqualsAndHashCode.Exclude  private Instrumentation instrumentation;

    @Override
    public void pushMessage(Pipeline jedisPipelined, RedisTtl redisTTL) {
        redisTTL.setTtl(jedisPipelined, key);
        instrumentation.logDebug("key: {}, value: {}", key, value);
        jedisPipelined.set(key, value);
    }

    @Override
    public void pushMessage(JedisCluster jedisCluster, RedisTtl redisTTL) {
        redisTTL.setTtl(jedisCluster, key);
        instrumentation.logDebug("key: {}, value: {}", key, value);
        jedisCluster.set(key, value);

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
