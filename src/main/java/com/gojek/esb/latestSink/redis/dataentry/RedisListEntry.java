package com.gojek.esb.latestSink.redis.dataentry;

import lombok.AllArgsConstructor;
import lombok.Getter;
import redis.clients.jedis.Pipeline;

/**
 * Class for Redis Hash set entry.
 */
@AllArgsConstructor
@Getter
public class RedisListEntry implements RedisDataEntry {
    private String key;
    private String value;

    @Override
    public void pushMessage(Pipeline jedisPipelined) {
        jedisPipelined.lpush(
                this.getKey(),
                this.getValue()
        );
    }
}
