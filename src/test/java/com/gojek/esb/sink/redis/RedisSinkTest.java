package com.gojek.esb.sink.redis;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.sink.redis.client.NoResponseException;
import com.gojek.esb.sink.redis.client.RedisClient;
import com.gojek.esb.sink.redis.dataentry.RedisDataEntry;
import com.gojek.esb.sink.redis.parsers.RedisParser;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RedisSinkTest {

    private RedisSink redisSink;
    private List<EsbMessage> esbMessages;
    private List<RedisDataEntry> redisDataEntry;

    @Mock
    private RedisClient redisClient;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private RedisParser redisMessageParser;

    @Before
    public void setUp() {
        esbMessages = Arrays.asList(new EsbMessage(new byte[0], new byte[0], "topic", 0, 100),
                new EsbMessage(new byte[0], new byte[0], "topic", 0, 100));

        redisSink = new RedisSink(redisClient, redisMessageParser, instrumentation);
    }

    @Test
    public void sendsMetricsForMessagesPushed() {
        redisSink.pushMessage(esbMessages);
        verify(instrumentation, times(1)).startExecution();
        verify(instrumentation, times(1)).captureExecutionTelemetry(esbMessages.size());
        InOrder inOrder = inOrder(instrumentation);
        inOrder.verify(instrumentation).startExecution();
        inOrder.verify(instrumentation).captureExecutionTelemetry(esbMessages.size());
    }

    @Test
    public void sendsMessagesToRedis() {
        redisSink.pushMessage(esbMessages);
        verify(redisClient, times(1)).execute(any());
    }

    @Test(expected = NoResponseException.class)
    public void shouldThrowAndInstrumentErrorWhenExecutionFail() {
        when(redisMessageParser.parse(esbMessages)).thenReturn(redisDataEntry);
        doThrow(NoResponseException.class).when(redisClient).execute(redisDataEntry);
        try {
            redisSink.pushMessage(esbMessages);
        } finally {
            verify(instrumentation, times(1)).captureClientError();
        }
    }

    @Test
    public void shouldCloseRedisClientOnClose() throws IOException {
        redisSink.close();
        verify(redisClient, times(1)).close();
    }
}
