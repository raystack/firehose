package com.gojek.esb.sink.redis.dataentry;

import com.gojek.esb.sink.redis.ttl.DurationTTL;
import com.gojek.esb.sink.redis.ttl.ExactTimeTTL;
import com.gojek.esb.sink.redis.ttl.NoRedisTTL;
import com.gojek.esb.sink.redis.ttl.RedisTTL;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import redis.clients.jedis.Pipeline;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class RedisHashSetFieldEntryTest {

    @Mock
    private Pipeline pipeline;

    private RedisTTL redisTTL;
    private RedisHashSetFieldEntry redisHashSetFieldEntry;

    @Before
    public void setup() {
        initMocks(this);
        redisTTL = new NoRedisTTL();
        redisHashSetFieldEntry = new RedisHashSetFieldEntry("test-key", "test-field", "test-value");
    }

    @Test
    public void shouldIOnlyPushDataWithoutTTLByDefault() {
        redisHashSetFieldEntry.pushMessage(pipeline, redisTTL);

        verify(pipeline, times(1)).hset("test-key", "test-field", "test-value");
        verify(pipeline, times(0)).expireAt(any(String.class), any(Long.class));
        verify(pipeline, times(0)).expireAt(any(String.class), any(Long.class));
    }

    @Test
    public void shouldSetProperTTLForExactTime() {
        redisTTL = new ExactTimeTTL(1000L);
        redisHashSetFieldEntry.pushMessage(pipeline, redisTTL);

        verify(pipeline, times(1)).expireAt("test-key", 1000L);
    }

    @Test
    public void shouldSetProperTTLForDuration() {
        redisTTL = new DurationTTL(1000);
        redisHashSetFieldEntry.pushMessage(pipeline, redisTTL);

        verify(pipeline, times(1)).expire("test-key", 1000);
    }
}
