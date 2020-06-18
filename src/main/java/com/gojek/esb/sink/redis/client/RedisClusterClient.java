package com.gojek.esb.sink.redis.client;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.sink.redis.dataentry.RedisDataEntry;
import com.gojek.esb.sink.redis.parsers.RedisParser;
import com.gojek.esb.sink.redis.ttl.RedisTTL;
import redis.clients.jedis.JedisCluster;

import java.util.ArrayList;
import java.util.List;

public class RedisClusterClient implements RedisClient {

    private RedisParser redisParser;
    private RedisTTL redisTTL;
    private JedisCluster jedisCluster;
    private List<RedisDataEntry> redisDataEntries;

    public RedisClusterClient(RedisParser redisParser, RedisTTL redisTTL, JedisCluster jedisCluster) {
        this.redisParser = redisParser;
        this.redisTTL = redisTTL;
        this.jedisCluster = jedisCluster;
    }

    @Override
    public void prepare(List<EsbMessage> esbMessages) {
        redisDataEntries = redisParser.parse(esbMessages);
    }

    @Override
    public List<EsbMessage> execute() {
        redisDataEntries.forEach(redisDataEntry -> redisDataEntry.pushMessage(jedisCluster, redisTTL));
        return new ArrayList<>();
    }

    @Override
    public void close() {
        jedisCluster.close();
    }
}
