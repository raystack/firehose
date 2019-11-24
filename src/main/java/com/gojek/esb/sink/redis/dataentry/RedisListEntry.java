package com.gojek.esb.sink.redis;

import com.gojek.esb.sink.redis.list.RedisDataEntry;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Class for Redis Hash set entry.
 */
@AllArgsConstructor
@Getter
public class RedisListEntry implements RedisDataEntry {
    private String key;
    private String value;
}
