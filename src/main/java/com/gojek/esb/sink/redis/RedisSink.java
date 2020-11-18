package com.gojek.esb.sink.redis;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.AbstractSink;
import com.gojek.esb.sink.redis.client.RedisClient;
import com.gojek.esb.sink.redis.exception.NoResponseException;

import java.util.List;

/**
 * RedisSink allows messages consumed from kafka to be persisted to a redis.
 * The related configurations for RedisSink can be found here: {@see com.gojek.esb.config.RedisSinkConfig}
 */

public class RedisSink extends AbstractSink {

    private RedisClient redisClient;

    public RedisSink(Instrumentation instrumentation, String sinkType, RedisClient redisClient) {
        super(instrumentation, sinkType);
        this.redisClient = redisClient;
    }

    @Override
    protected void prepare(List<EsbMessage> esbMessages) {
        redisClient.prepare(esbMessages);
    }

    @Override
    protected List<EsbMessage> execute() throws NoResponseException {
        return redisClient.execute();
    }

    @Override
    public void close() {
        getInstrumentation().logInfo("Redis connection closing");
        redisClient.close();
    }
}
