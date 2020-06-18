package com.gojek.esb.sink.redis.client;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.sink.redis.dataentry.RedisDataEntry;
import com.gojek.esb.sink.redis.exception.NoResponseException;
import com.gojek.esb.sink.redis.parsers.RedisParser;
import com.gojek.esb.sink.redis.ttl.RedisTTL;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.List;

public class RedisStandaloneClient implements RedisClient {

    private RedisParser redisParser;
    private RedisTTL redisTTL;
    private Jedis jedis;
    private Pipeline jedisPipelined;

    public RedisStandaloneClient(RedisParser redisParser, RedisTTL redisTTL, Jedis jedis) {
        this.redisParser = redisParser;
        this.redisTTL = redisTTL;
        this.jedis = jedis;
    }

    @Override
    public void prepare(List<EsbMessage> esbMessages) {
        List<RedisDataEntry> redisDataEntries = redisParser.parse(esbMessages);
        jedisPipelined = jedis.pipelined();

        jedisPipelined.multi();
        redisDataEntries.forEach(redisDataEntry -> redisDataEntry.pushMessage(jedisPipelined, redisTTL));
    }

    @Override
    public List<EsbMessage> execute() {
        Response<List<Object>> responses = jedisPipelined.exec();
        jedisPipelined.sync();
        if (responses.get() == null || responses.get().isEmpty()) {
            throw new NoResponseException();
        }
        return new ArrayList<>();

    }

    @Override
    public void close() {
        jedis.close();
    }
}
