package com.gojek.esb.sink.redis.dataentry;

import com.gojek.esb.sink.redis.ttl.RedisTTL;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

public interface RedisDataEntry {

    void pushMessage(Pipeline jedisPipelined, RedisTTL redisTTL);

    void pushMessage(JedisCluster jedisCluster, RedisTTL redisTTL);
}
