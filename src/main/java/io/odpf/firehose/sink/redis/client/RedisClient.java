package io.odpf.firehose.sink.redis.client;

import io.odpf.firehose.consumer.Message;

import java.util.List;

public interface RedisClient {
    void prepare(List<Message> messages);

    List<Message> execute();

    void close();
}
