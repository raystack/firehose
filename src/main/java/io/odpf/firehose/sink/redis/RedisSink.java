package io.odpf.firehose.sink.redis;

import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.redis.client.RedisClient;
import io.odpf.firehose.sink.redis.exception.NoResponseException;

import java.util.List;

/**
 * RedisSink allows messages consumed from kafka to be persisted to a redis.
 * The related configurations for RedisSink can be found here: {@see io.odpf.firehose.config.RedisSinkConfig}
 */
public class RedisSink extends AbstractSink {

    private RedisClient redisClient;

    /**
     * Instantiates a new Redis sink.
     *
     * @param firehoseInstrumentation the instrumentation
     * @param sinkType        the sink type
     * @param redisClient     the redis client
     */
    public RedisSink(FirehoseInstrumentation firehoseInstrumentation, String sinkType, RedisClient redisClient) {
        super(firehoseInstrumentation, sinkType);
        this.redisClient = redisClient;
    }

    /**
     * process messages before sending to redis.
     *
     * @param messages the messages
     */
    @Override
    protected void prepare(List<Message> messages) {
        redisClient.prepare(messages);
    }

    /**
     * Send data to redis.
     *
     * @return the list
     * @throws NoResponseException the no response exception
     */
    @Override
    protected List<Message> execute() throws NoResponseException {
        return redisClient.execute();
    }

    @Override
    public void close() {
        getFirehoseInstrumentation().logInfo("Redis connection closing");
        redisClient.close();
    }
}
