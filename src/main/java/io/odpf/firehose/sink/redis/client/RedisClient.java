package io.odpf.firehose.sink.redis.client;

import io.odpf.firehose.message.Message;

import java.util.List;

/**
 * Redis client interface to be used in RedisSink.
 */
public interface RedisClient {
    /**
     * Process messages before sending.
     *
     * @param messages the messages
     */
    void prepare(List<Message> messages);

    /**
     * Sends the processed messages to redis.
     *
     * @return list of messages
     */
    List<Message> execute();

    /**
     * Close the client.
     */
    void close();
}
