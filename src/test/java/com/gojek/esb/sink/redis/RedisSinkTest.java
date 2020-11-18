package com.gojek.esb.sink.redis;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.redis.client.RedisClient;
import com.gojek.esb.sink.redis.exception.NoResponseException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RedisSinkTest {
    @Mock
    private RedisClient redisClient;
    @Mock
    private Instrumentation instrumentation;
    private RedisSink redis;

    @Before
    public void setup() {
        redis = new RedisSink(instrumentation, "redis", redisClient);
    }

    @Test
    public void shouldInvokeExecuteOnTheClient() {
        redis.execute();

        verify(redisClient).execute();
    }

    @Test
    public void shouldInvokePrepareOnTheClient() {
        ArrayList<EsbMessage> esbMessages = new ArrayList<>();

        redis.prepare(esbMessages);

        verify(redisClient).prepare(esbMessages);
    }

    @Test
    public void shouldInvokeCloseOnTheClient() {
        redis.close();

        verify(redisClient).close();
    }

    @Test
    public void shouldLogWhenClosingConnection() {
        redis.close();

        verify(instrumentation, times(1)).logInfo("Redis connection closing");
    }

    @Test
    public void sendsMetricsForSuccessMessages() {
        ArrayList<EsbMessage> esbMessages = new ArrayList<>();

        redis.pushMessage(esbMessages);

        verify(instrumentation, times(1)).capturePreExecutionLatencies(esbMessages);
        verify(instrumentation, times(1)).startExecution();
        verify(instrumentation, times(1)).logDebug("Preparing {} messages", esbMessages.size());
        verify(instrumentation, times(1)).captureSuccessExecutionTelemetry(any(), any());
        InOrder inOrder = inOrder(instrumentation);
        inOrder.verify(instrumentation).logDebug("Preparing {} messages", esbMessages.size());
        inOrder.verify(instrumentation).capturePreExecutionLatencies(esbMessages);
        inOrder.verify(instrumentation).startExecution();
        inOrder.verify(instrumentation).captureSuccessExecutionTelemetry(any(), any());
    }

    @Test
    public void sendsMetricsForFailedMessages() {
        when(redisClient.execute()).thenThrow(new NoResponseException());
        ArrayList<EsbMessage> esbMessages = new ArrayList<>();

        redis.pushMessage(esbMessages);

        verify(instrumentation, times(1)).capturePreExecutionLatencies(esbMessages);
        verify(instrumentation, times(1)).startExecution();
        verify(instrumentation, times(1)).logDebug("Preparing {} messages", esbMessages.size());
        verify(instrumentation, times(1)).captureFailedExecutionTelemetry(any(), any());
        InOrder inOrder = inOrder(instrumentation);
        inOrder.verify(instrumentation).logDebug("Preparing {} messages", esbMessages.size());
        inOrder.verify(instrumentation).capturePreExecutionLatencies(esbMessages);
        inOrder.verify(instrumentation).startExecution();
        inOrder.verify(instrumentation).captureFailedExecutionTelemetry(any(), any());
    }


}
