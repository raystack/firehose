package com.gojek.esb.sink.redis.client;

import com.gojek.esb.consumer.Message;

import java.util.List;

public interface RedisClient {
    void prepare(List<Message> messages);

    List<Message> execute();

    void close();
}
