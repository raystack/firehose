package com.gojek.esb.sink.redis.client;

import com.gojek.esb.consumer.Message;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.redis.dataentry.RedisDataEntry;
import com.gojek.esb.sink.redis.exception.NoResponseException;
import com.gojek.esb.sink.redis.parsers.RedisParser;
import com.gojek.esb.sink.redis.ttl.RedisTtl;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.List;

public class RedisStandaloneClient implements RedisClient {

    private Instrumentation instrumentation;
    private RedisParser redisParser;
    private RedisTtl redisTTL;
    private Jedis jedis;
    private Pipeline jedisPipelined;

    public RedisStandaloneClient(Instrumentation instrumentation, RedisParser redisParser, RedisTtl redisTTL, Jedis jedis) {
        this.instrumentation = instrumentation;
        this.redisParser = redisParser;
        this.redisTTL = redisTTL;
        this.jedis = jedis;
    }

    @Override
    public void prepare(List<Message> messages) {
        List<RedisDataEntry> redisDataEntries = redisParser.parse(messages);
        jedisPipelined = jedis.pipelined();

        jedisPipelined.multi();
        redisDataEntries.forEach(redisDataEntry -> redisDataEntry.pushMessage(jedisPipelined, redisTTL));
    }

    @Override
    public List<Message> execute() {
        Response<List<Object>> responses = jedisPipelined.exec();
        instrumentation.logDebug("jedis responses: {}", responses);
        jedisPipelined.sync();
        if (responses.get() == null || responses.get().isEmpty()) {
            throw new NoResponseException();
        }
        return new ArrayList<>();
    }

    @Override
    public void close() {
        instrumentation.logInfo("Closing Jedis client");
        jedis.close();
    }
}
