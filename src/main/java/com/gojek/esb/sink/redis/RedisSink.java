package com.gojek.esb.sink.redis;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.AbstractSink;
import com.gojek.esb.sink.redis.dataentry.RedisDataEntry;
import com.gojek.esb.sink.redis.exception.NoResponseException;
import com.gojek.esb.sink.redis.parsers.RedisParser;
import com.gojek.esb.sink.redis.ttl.RedisTTL;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.List;

/**
 * RedisSink allows messages consumed from kafka to be persisted to a redis.
 * The related configurations for RedisSink can be found here: {@see com.gojek.esb.config.RedisSinkConfig}
 */

public class RedisSink extends AbstractSink {

    private RedisParser redisParser;
    private List<RedisDataEntry> redisDataEntries;
    private Jedis jedis;
    private RedisTTL redisTTL;
    private Pipeline jedisPipelined;

    public RedisSink(Instrumentation instrumentation, String sinkType, RedisParser redisParser, Jedis jedis, RedisTTL redisTTL) {
        super(instrumentation, sinkType);
        this.redisParser = redisParser;
        this.jedis = jedis;
        this.redisTTL = redisTTL;
    }

    public RedisSink(Instrumentation instrumentation, String sinkType, RedisParser redisParser, Jedis jedis, Pipeline jedisPipelined, RedisTTL redisTTL) {
        super(instrumentation, sinkType);
        this.redisParser = redisParser;
        this.jedis = jedis;
        this.jedisPipelined = jedisPipelined;
        this.redisTTL = redisTTL;
    }


    @Override
    protected void prepare(List<EsbMessage> esbMessages) {
        redisDataEntries = redisParser.parse(esbMessages);
        jedisPipelined = jedis.pipelined();
        jedisPipelined.multi();

        redisDataEntries.forEach(redisDataEntry -> {
            redisDataEntry.pushMessage(jedisPipelined, redisTTL);
        });

    }

    @Override
    protected List<EsbMessage> execute() throws NoResponseException {
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
