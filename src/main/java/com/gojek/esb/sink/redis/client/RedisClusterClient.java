package com.gojek.esb.sink.redis.client;

import com.gojek.esb.consumer.Message;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.redis.dataentry.RedisDataEntry;
import com.gojek.esb.sink.redis.parsers.RedisParser;
import com.gojek.esb.sink.redis.ttl.RedisTtl;
import redis.clients.jedis.JedisCluster;

import java.util.ArrayList;
import java.util.List;

public class RedisClusterClient implements RedisClient {

    private Instrumentation instrumentation;
    private RedisParser redisParser;
    private RedisTtl redisTTL;
    private JedisCluster jedisCluster;
    private List<RedisDataEntry> redisDataEntries;

    public RedisClusterClient(Instrumentation instrumentation, RedisParser redisParser, RedisTtl redisTTL, JedisCluster jedisCluster) {
        this.instrumentation = instrumentation;
        this.redisParser = redisParser;
        this.redisTTL = redisTTL;
        this.jedisCluster = jedisCluster;
    }

    @Override
    public void prepare(List<Message> messages) {
        redisDataEntries = redisParser.parse(messages);
    }

    @Override
    public List<Message> execute() {
        redisDataEntries.forEach(redisDataEntry -> redisDataEntry.pushMessage(jedisCluster, redisTTL));
        return new ArrayList<>();
    }

    @Override
    public void close() {
        instrumentation.logInfo("Closing Jedis client");
        jedisCluster.close();
    }
}
