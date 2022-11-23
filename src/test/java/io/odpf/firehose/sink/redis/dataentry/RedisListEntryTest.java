package io.odpf.firehose.sink.redis.dataentry;

import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.firehose.sink.redis.ttl.DurationTtl;
import io.odpf.firehose.sink.redis.ttl.ExactTimeTtl;
import io.odpf.firehose.sink.redis.ttl.NoRedisTtl;
import io.odpf.firehose.sink.redis.ttl.RedisTtl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RedisListEntryTest {

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    @Mock
    private Pipeline pipeline;

    @Mock
    private JedisCluster jedisCluster;

    private RedisTtl redisTTL;
    private RedisListEntry redisListEntry;

    @Before
    public void setup() {
        redisTTL = new NoRedisTtl();
        redisListEntry = new RedisListEntry("test-key", "test-value", firehoseInstrumentation);
    }

    @Test
    public void shouldIOnlyPushDataWithoutTTLByDefaultForPipeline() {
        redisListEntry.pushMessage(pipeline, redisTTL);

        verify(pipeline, times(1)).lpush("test-key", "test-value");
        verify(pipeline, times(0)).expireAt(any(String.class), any(Long.class));
        verify(pipeline, times(0)).expireAt(any(String.class), any(Long.class));
        verify(firehoseInstrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
    }

    @Test
    public void shouldSetProperTTLForExactTimeForPipeline() {
        redisTTL = new ExactTimeTtl(1000L);
        redisListEntry.pushMessage(pipeline, redisTTL);

        verify(pipeline, times(1)).expireAt("test-key", 1000L);
        verify(firehoseInstrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
    }

    @Test
    public void shouldSetProperTTLForDurationForPipeline() {
        redisTTL = new DurationTtl(1000);
        redisListEntry.pushMessage(pipeline, redisTTL);

        verify(pipeline, times(1)).expire("test-key", 1000);
        verify(firehoseInstrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
    }

    @Test
    public void shouldIOnlyPushDataWithoutTTLByDefaultForCluster() {
        redisListEntry.pushMessage(jedisCluster, redisTTL);

        verify(jedisCluster, times(1)).lpush("test-key", "test-value");
        verify(jedisCluster, times(0)).expireAt(any(String.class), any(Long.class));
        verify(jedisCluster, times(0)).expireAt(any(String.class), any(Long.class));
        verify(firehoseInstrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
    }

    @Test
    public void shouldSetProperTTLForExactTimeForCluster() {
        redisTTL = new ExactTimeTtl(1000L);
        redisListEntry.pushMessage(jedisCluster, redisTTL);

        verify(jedisCluster, times(1)).expireAt("test-key", 1000L);
        verify(firehoseInstrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
    }

    @Test
    public void shouldSetProperTTLForDurationForCluster() {
        redisTTL = new DurationTtl(1000);
        redisListEntry.pushMessage(jedisCluster, redisTTL);

        verify(jedisCluster, times(1)).expire("test-key", 1000);
        verify(firehoseInstrumentation, times(1)).logDebug("key: {}, value: {}", "test-key", "test-value");
    }
}
