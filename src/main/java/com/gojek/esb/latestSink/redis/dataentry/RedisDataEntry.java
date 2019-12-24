package com.gojek.esb.latestSink.redis.dataentry;

import redis.clients.jedis.Pipeline;

public interface RedisDataEntry {

    void pushMessage(Pipeline jedisPipelined);
}
