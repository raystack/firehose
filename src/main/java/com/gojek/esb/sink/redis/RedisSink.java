package com.gojek.esb.sink.redis;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.sink.redis.dataentry.RedisDataEntry;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.gojek.esb.metrics.Metrics.*;

/**
 * RedisSink allows messages consumed from kafka to be persisted to a redis.
 * The related configurations for RedisSink can be found here: {@see com.gojek.esb.config.RedisSinkConfig}
 */
@AllArgsConstructor
public class RedisSink implements Sink {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisSink.class);
    private RedisClient redisClient;
    private RedisMessageParser redisMessageParser;
    private StatsDReporter statsDReporter;

    @Override
    public List<EsbMessage> pushMessage(List<EsbMessage> esbMessages) {
        Instant startExecution = statsDReporter.getClock().now();
        List<RedisDataEntry> redisDataEntries = getRedisDataEntries(esbMessages);
        redisClient.execute(redisDataEntries);
        LOGGER.info("Pushed {} messages to redis.", esbMessages.size());
        statsDReporter.captureDurationSince(REDIS_SINK_WRITE_TIME, startExecution);
        statsDReporter.captureCount(REDIS_SINK_MESSAGES_COUNT, esbMessages.size(), SUCCESS_TAG);
        return new ArrayList<>();
    }

    private List<RedisDataEntry> getRedisDataEntries(List<EsbMessage> esbMessages) {
        return esbMessages
                .stream()
                .map(esbMessage -> redisMessageParser.parse(esbMessage))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public void close() throws IOException {
        redisClient.close();
    }

}
