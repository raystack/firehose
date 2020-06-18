package com.gojek.esb.sink.redis.client;

import com.gojek.esb.consumer.EsbMessage;

import java.util.List;

public interface RedisClient {
    void prepare(List<EsbMessage> esbMessages);

    List<EsbMessage> execute();

    void close();
}
