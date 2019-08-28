package com.gojek.esb.sink.redis;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Class for Redis Hash set entry.
 */
@AllArgsConstructor
@Getter
public class RedisHashSetFieldEntry {

    private String key;
    private String field;
    private String value;

}
