package com.gojek.esb.sink.redis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.sink.redis.client.NoResponseException;
import com.gojek.esb.sink.redis.client.RedisClient;
import com.gojek.esb.sink.redis.dataentry.RedisDataEntry;
import com.gojek.esb.sink.redis.parsers.RedisParser;

import lombok.AllArgsConstructor;

/**
 * RedisSink allows messages consumed from kafka to be persisted to a redis.
 * The related configurations for RedisSink can be found here: {@see com.gojek.esb.config.RedisSinkConfig}
 */
@AllArgsConstructor
public class RedisSink implements Sink {

    private RedisClient redisClient;
    private RedisParser redisParser;
    private Instrumentation instrumentation;

    @Override
    public List<EsbMessage> pushMessage(List<EsbMessage> esbMessages) {
        instrumentation.startExecution();
        List<RedisDataEntry> redisDataEntries = redisParser.parse(esbMessages);
        try {
            redisClient.execute(redisDataEntries);
        } catch (NoResponseException e) {
            instrumentation.captureFatalError(e, "Redis Pipeline error: no responds received");
            throw e;
        }
        instrumentation.captureSuccessExecutionTelemetry("redis", esbMessages);
        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        redisClient.close();
    }
}
