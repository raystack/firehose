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

public class RedisListEntryTest {

    @Mock
    private Pipeline pipeline;

    private RedisTTL redisTTL;
    private RedisListEntry redisListEntry;

    @Before
    public void setup() {
        initMocks(this);
        redisTTL = new NoRedisTTL();
        redisListEntry = new RedisListEntry("test-key", "test-value");
    }

    @Test
    public void shouldIOnlyPushDataWithoutTTLByDefault() {
        redisListEntry.pushMessage(pipeline, redisTTL);

        verify(pipeline, times(1)).lpush("test-key", "test-value");
        verify(pipeline, times(0)).expireAt(any(String.class), any(Long.class));
        verify(pipeline, times(0)).expireAt(any(String.class), any(Long.class));
    }

    @Test
    public void shouldSetProperTTLForExactTime() {
        redisTTL = new ExactTimeTTL(1000L);
        redisListEntry.pushMessage(pipeline, redisTTL);

        verify(pipeline, times(1)).expireAt("test-key", 1000L);
    }

    @Test
    public void shouldSetProperTTLForDuration() {
        redisTTL = new DurationTTL(1000);
        redisListEntry.pushMessage(pipeline, redisTTL);

        verify(pipeline, times(1)).expire("test-key", 1000);
    }
}
