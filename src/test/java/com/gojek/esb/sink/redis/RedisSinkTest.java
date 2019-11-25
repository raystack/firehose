package com.gojek.esb.sink.redis;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.redis.parsers.RedisParser;
import com.gojek.esb.util.Clock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RedisSinkTest {

    private RedisSink redisSink;
    private List<EsbMessage> esbMessages;


    @Mock
    private RedisClient redisClient;

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private RedisParser redisMessageParser;

    @Before
    public void setUp() {
        when(statsDReporter.getClock()).thenReturn(new Clock());

        esbMessages = Arrays.asList(new EsbMessage(new byte[0], new byte[0], "topic", 0, 100),
                new EsbMessage(new byte[0], new byte[0], "topic", 0, 100));

        redisSink = new RedisSink(redisClient, redisMessageParser, statsDReporter);
    }

    @Test
    public void sendsMetricsForMessagesPushed() {
        redisSink.pushMessage(esbMessages);
        verify(statsDReporter, times(1)).captureCount(any(), any(), any());
        verify(statsDReporter, times(1)).captureDurationSince(any(), any(), any());
    }

    @Test
    public void sendsMessagesToRedis() {
        redisSink.pushMessage(esbMessages);
        verify(redisClient, times(1)).execute(any());
    }

    @Test
    public void shouldCloseRedisClientOnClose() throws IOException {
        redisSink.close();
        verify(redisClient, times(1)).close();
    }
}
