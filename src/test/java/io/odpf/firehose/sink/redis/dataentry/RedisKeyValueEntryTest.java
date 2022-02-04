package io.odpf.firehose.sink.redis.dataentry;

import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.redis.ttl.DurationTtl;
import io.odpf.firehose.sink.redis.ttl.NoRedisTtl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RedisKeyValueEntryTest {
    @Mock
    private Instrumentation instrumentation;

    @Mock
    private Pipeline pipeline;

    @Mock
    private JedisCluster jedisCluster;

    private InOrder inOrderPipeline;
    private InOrder inOrderJedis;


    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        inOrderPipeline = Mockito.inOrder(pipeline);
        inOrderJedis = Mockito.inOrder(jedisCluster);

    }

    @Test
    public void pushMessageWithNoTtl() {
        String key = "key";
        String value = "value";
        RedisKeyValueEntry redisKeyValueEntry = new RedisKeyValueEntry(key, value, instrumentation);
        redisKeyValueEntry.pushMessage(pipeline, new NoRedisTtl());
        inOrderPipeline.verify(pipeline, times(1)).set(key, value);
        inOrderPipeline.verify(pipeline, times(0)).expireAt(any(String.class), any(Long.class));

    }

    @Test
    public void pushMessageWithTtl() {
        String key = "key";
        String value = "value";
        RedisKeyValueEntry redisKeyValueEntry = new RedisKeyValueEntry(key, value, instrumentation);
        redisKeyValueEntry.pushMessage(pipeline, new DurationTtl(100));
        inOrderPipeline.verify(pipeline, times(1)).set(key, value);
        inOrderPipeline.verify(pipeline, times(1)).expire(key, 100);
    }

    @Test
    public void pushMessageVerifyInstrumentation() {
        String key = "this-key";
        String value = "john";
        RedisKeyValueEntry redisKeyValueEntry = new RedisKeyValueEntry(key, value, instrumentation);
        redisKeyValueEntry.pushMessage(pipeline, new DurationTtl(100));
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", key, value);
    }


    @Test
    public void pushMessageWithNoTtlUsingJedisCluster() {
        String key = "key";
        String value = "value";
        RedisKeyValueEntry redisKeyValueEntry = new RedisKeyValueEntry(key, value, instrumentation);
        redisKeyValueEntry.pushMessage(jedisCluster, new NoRedisTtl());
        inOrderJedis.verify(jedisCluster, times(1)).set(key, value);
        inOrderJedis.verify(jedisCluster, times(0)).expireAt(any(String.class), any(Long.class));

    }

    @Test
    public void pushMessageWithTtlUsingJedisCluster() {
        String key = "key";
        String value = "value";
        RedisKeyValueEntry redisKeyValueEntry = new RedisKeyValueEntry(key, value, instrumentation);
        redisKeyValueEntry.pushMessage(jedisCluster, new DurationTtl(100));
        inOrderJedis.verify(jedisCluster, times(1)).set(key, value);
        inOrderJedis.verify(jedisCluster, times(1)).expire(key, 100);
    }

    @Test
    public void pushMessageVerifyInstrumentationUsingJedisCluster() {
        String key = "this-key";
        String value = "john";
        RedisKeyValueEntry redisKeyValueEntry = new RedisKeyValueEntry(key, value, instrumentation);
        redisKeyValueEntry.pushMessage(jedisCluster, new DurationTtl(100));
        verify(instrumentation, times(1)).logDebug("key: {}, value: {}", key, value);
    }

}
