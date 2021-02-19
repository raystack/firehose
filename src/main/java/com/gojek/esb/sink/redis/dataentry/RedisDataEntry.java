package com.gojek.esb.sink.redis.dataentry;

import com.gojek.esb.sink.redis.ttl.RedisTtl;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

public interface RedisDataEntry {

    void pushMessage(Pipeline jedisPipelined, RedisTtl redisTTL);

    void pushMessage(JedisCluster jedisCluster, RedisTtl redisTTL);
}
