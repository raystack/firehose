package com.gojek.esb.sink.redis.dataentry;

import redis.clients.jedis.Pipeline;

public interface RedisDataEntry {

    void pushMessage(Pipeline jedisPipelined);
}
