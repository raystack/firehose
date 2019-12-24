package com.gojek.esb.latestSink.redis.dataentry;

import lombok.AllArgsConstructor;
import lombok.Getter;
import redis.clients.jedis.Pipeline;

/**
 * Class for Redis Hash set entry.
 */
@AllArgsConstructor
@Getter
public class RedisHashSetFieldEntry implements RedisDataEntry {

    private String key;
    private String field;
    private String value;


    @Override
    public void pushMessage(Pipeline jedisPipelined) {
        jedisPipelined.hset(
                this.getKey(),
                this.getField(),
                this.getValue()
        );
    }
}
