package com.gojek.esb.sink.redis;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RedisClientTest {

    private RedisClient redisClient;

    @Mock
    private Jedis jedis;

    @Mock
    private Pipeline jedisPipeline;

    @Mock
    private Response<List<Object>> responses;

    @Before
    public void setUp() {
        when(jedis.pipelined()).thenReturn(jedisPipeline);
        when(jedisPipeline.exec()).thenReturn(responses);
        when(responses.get()).thenReturn(Collections.singletonList("MOCK_LIST_ITEM"));

        redisClient = new RedisClient(jedis);
    }


    @Test
    public void shouldSetHashSetEntry() {

        List<RedisHashSetFieldEntry> entryList = Collections.singletonList(new RedisHashSetFieldEntry("order-123", "driver_id", "driver-234"));

        redisClient.executeHash(entryList);
        verify(jedisPipeline, times(1)).hset("order-123", "driver_id", "driver-234");
    }

    @Test(expected = RuntimeException.class)
    public void shouldThroughRuntimeExceptionWhenNoResponseFromRedis() {
        when(responses.get()).thenReturn(null);

        List<RedisHashSetFieldEntry> entryList = Collections.singletonList(new RedisHashSetFieldEntry("order-123", "driver_id", "driver-234"));

        redisClient.executeHash(entryList);
    }

    @Test
    public void shouldCloseJedisClientOnClose() throws IOException {
        redisClient.close();
        verify(jedis, times(1)).close();
    }
}
