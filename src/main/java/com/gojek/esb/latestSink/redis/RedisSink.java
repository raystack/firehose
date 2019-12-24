package com.gojek.esb.latestSink.redis;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.latestSink.AbstractSink;
import com.gojek.esb.latestSink.redis.dataentry.RedisDataEntry;
import com.gojek.esb.latestSink.redis.parsers.RedisParser;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.latestSink.redis.exception.NoResponseException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.io.IOException;
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
    private Pipeline jedisPipelined;

    public RedisSink(Instrumentation instrumentation, String sinkType, RedisParser redisParser, Jedis jedis) {
        super(instrumentation, sinkType);
        this.redisParser = redisParser;
        this.jedis = jedis;
    }

    @Override
    protected void prepare(List<EsbMessage> esbMessages) throws DeserializerException, IOException {
        redisDataEntries = redisParser.parse(esbMessages);
        jedisPipelined = jedis.pipelined();
        jedisPipelined.multi();

        redisDataEntries.forEach(redisDataEntry -> {
            redisDataEntry.pushMessage(jedisPipelined);
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
