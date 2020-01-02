package com.gojek.esb.sink.redis;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.redis.dataentry.RedisDataEntry;
import com.gojek.esb.sink.redis.dataentry.RedisHashSetFieldEntry;
import com.gojek.esb.sink.redis.dataentry.RedisListEntry;
import com.gojek.esb.sink.redis.exception.NoResponseException;
import com.gojek.esb.sink.redis.parsers.RedisParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RedisSinkTest {

    private RedisSink redisSink;
    private List<EsbMessage> esbMessages;
    private List<RedisDataEntry> redisDataEntries;

    @Mock
    private RedisDataEntry redisDataEntry;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private RedisParser redisMessageParser;

    @Mock
    private Jedis jedis;

    @Mock
    private Pipeline jedisPipeline;

    @Mock
    private Response<List<Object>> responses;


    @Before
    public void setUp() {
        esbMessages = Arrays.asList(new EsbMessage(new byte[0], new byte[0], "topic", 0, 100),
                new EsbMessage(new byte[0], new byte[0], "topic", 0, 100));

        redisSink = new RedisSink(instrumentation, "redis", redisMessageParser, jedis);

        redisDataEntries = new ArrayList<>();

        RedisDataEntry redisListEntry1 = new RedisListEntry("key1", "value1");
        RedisDataEntry redisListEntry2 = new RedisListEntry("key2", "value2");
        redisDataEntries.add(redisListEntry1);
        redisDataEntries.add(redisListEntry2);

        when(jedis.pipelined()).thenReturn(jedisPipeline);
        when(jedisPipeline.exec()).thenReturn(responses);
        when(responses.get()).thenReturn(Collections.singletonList("MOCK_LIST_ITEM"));
        when(redisMessageParser.parse(esbMessages)).thenReturn(redisDataEntries);
    }

    @Test
    public void sendsMetricsForSuccessfullyPushedMessages() throws IOException, DeserializerException {
        redisSink.pushMessage(esbMessages);
        verify(instrumentation, times(1)).lifetimeTillSink(esbMessages);
        verify(instrumentation, times(1)).startExecution();
        verify(instrumentation, times(1)).logInfo("pushing {} messages", esbMessages.size());
        verify(instrumentation, times(1)).captureSuccessExecutionTelemetry("redis", esbMessages);
        InOrder inOrder = inOrder(instrumentation);
        inOrder.verify(instrumentation).lifetimeTillSink(esbMessages);
        inOrder.verify(instrumentation).startExecution();
        inOrder.verify(instrumentation).logInfo("pushing {} messages", esbMessages.size());
        inOrder.verify(instrumentation).captureSuccessExecutionTelemetry("redis", esbMessages);
    }

    @Test
    public void sendsMetricsForFailedMessages() throws IOException, DeserializerException {
        when(responses.get()).thenReturn(null);
        redisSink.pushMessage(esbMessages);

        verify(instrumentation, times(1)).lifetimeTillSink(esbMessages);
        verify(instrumentation, times(1)).startExecution();
        verify(instrumentation, times(1)).logInfo("pushing {} messages", esbMessages.size());
        verify(instrumentation, times(1)).captureFailedExecutionTelemetry(any(), any());
        InOrder inOrder = inOrder(instrumentation);
        inOrder.verify(instrumentation).lifetimeTillSink(esbMessages);
        inOrder.verify(instrumentation).startExecution();
        inOrder.verify(instrumentation).logInfo("pushing {} messages", esbMessages.size());
        inOrder.verify(instrumentation).captureFailedExecutionTelemetry(any(), any());
    }


    @Test
    public void shouldSendMessagesForList() throws IOException, DeserializerException {
        redisSink.pushMessage(esbMessages);

        verify(jedisPipeline, times(1)).lpush("key1", "value1");
        verify(jedisPipeline, times(1)).lpush("key2", "value2");
        verify(jedisPipeline, times(1)).sync();

    }

    @Test(expected = NoResponseException.class)
    public void shouldThrowAndInstrumentErrorWhenExecutionFail() throws IOException, DeserializerException {
        when(redisMessageParser.parse(esbMessages)).thenReturn(redisDataEntries);
        when(responses.get()).thenReturn(null);

        RedisSink redisSinkStub = new RedisSink(instrumentation, "redis", redisMessageParser, jedis, jedisPipeline);
        redisSinkStub.execute();
    }

    @Test
    public void shouldExecuteSuccessfully() {
        RedisSink redisSinkStub = new RedisSink(instrumentation, "redis", redisMessageParser, jedis, jedisPipeline);
        List<EsbMessage> execute = redisSinkStub.execute();

        verify(jedisPipeline, times(1)).sync();
        Assert.assertEquals(0, execute.size());
    }

    @Test
    public void shouldSendMessagesForHashSetEntry() throws IOException, DeserializerException {
        List<RedisDataEntry> entryList = Collections.singletonList(new RedisHashSetFieldEntry("order-123", "driver_id", "driver-234"));
        when(redisMessageParser.parse(esbMessages)).thenReturn(entryList);

        redisSink.pushMessage(esbMessages);
        verify(jedisPipeline, times(entryList.size())).hset("order-123", "driver_id", "driver-234");
        verify(jedisPipeline, times(1)).sync();
    }

    @Test
    public void shouldCloseRedisClientOnClose() {
        redisSink.close();
        verify(jedis, times(1)).close();
    }
}
