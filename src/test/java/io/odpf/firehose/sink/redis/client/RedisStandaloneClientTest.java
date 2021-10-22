package io.odpf.firehose.sink.redis.client;

import io.odpf.firehose.message.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.redis.dataentry.RedisDataEntry;
import io.odpf.firehose.sink.redis.dataentry.RedisHashSetFieldEntry;
import io.odpf.firehose.sink.redis.dataentry.RedisListEntry;
import io.odpf.firehose.sink.redis.exception.NoResponseException;
import io.odpf.firehose.sink.redis.parsers.RedisParser;
import io.odpf.firehose.sink.redis.ttl.RedisTtl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RedisStandaloneClientTest {
    @Mock
    private StatsDReporter statsDReporter;
    @Mock
    private Instrumentation instrumentation;

    private final RedisHashSetFieldEntry firstRedisSetEntry = new RedisHashSetFieldEntry("key1", "field1", "value1", new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class));
    private final RedisHashSetFieldEntry secondRedisSetEntry = new RedisHashSetFieldEntry("key2", "field2", "value2", new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class));
    private final RedisListEntry firstRedisListEntry = new RedisListEntry("key1", "value1", new Instrumentation(statsDReporter, RedisListEntry.class));
    private final RedisListEntry secondRedisListEntry = new RedisListEntry("key2", "value2", new Instrumentation(statsDReporter, RedisListEntry.class));
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private RedisClient redisClient;
    private List<Message> messages;
    private List<RedisDataEntry> redisDataEntries;
    @Mock
    private RedisParser redisMessageParser;

    @Mock
    private RedisTtl redisTTL;

    @Mock
    private Jedis jedis;

    @Mock
    private Pipeline jedisPipeline;

    @Mock
    private Response<List<Object>> responses;


    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        messages = Arrays.asList(new Message(new byte[0], new byte[0], "topic", 0, 100),
                new Message(new byte[0], new byte[0], "topic", 0, 100));

        redisClient = new RedisStandaloneClient(instrumentation, redisMessageParser, redisTTL, jedis);

        redisDataEntries = new ArrayList<>();

        when(jedis.pipelined()).thenReturn(jedisPipeline);
        when(redisMessageParser.parse(messages)).thenReturn(redisDataEntries);
    }

    @Test
    public void pushesDataEntryForListInATransaction() throws DeserializerException {
        populateRedisDataEntry(firstRedisListEntry, secondRedisListEntry);

        redisClient.prepare(messages);

        verify(jedisPipeline, times(1)).multi();
        verify(jedisPipeline).lpush(firstRedisListEntry.getKey(), firstRedisListEntry.getValue());
        verify(jedisPipeline).lpush(secondRedisListEntry.getKey(), secondRedisListEntry.getValue());
    }

    @Test
    public void setsTTLForListItemsInATransaction() throws DeserializerException {
        populateRedisDataEntry(firstRedisListEntry, secondRedisListEntry);

        redisClient.prepare(messages);

        verify(redisTTL).setTtl(jedisPipeline, firstRedisListEntry.getKey());
        verify(redisTTL).setTtl(jedisPipeline, secondRedisListEntry.getKey());
    }

    @Test
    public void pushesDataEntryForSetInATransaction() throws DeserializerException {
        populateRedisDataEntry(firstRedisSetEntry, secondRedisSetEntry);

        redisClient.prepare(messages);

        verify(jedisPipeline, times(1)).multi();
        verify(jedisPipeline).hset(firstRedisSetEntry.getKey(), firstRedisSetEntry.getField(), firstRedisSetEntry.getValue());
        verify(jedisPipeline).hset(secondRedisSetEntry.getKey(), secondRedisSetEntry.getField(), secondRedisSetEntry.getValue());
    }

    @Test
    public void setsTTLForSetItemsInATransaction() throws DeserializerException {
        populateRedisDataEntry(firstRedisSetEntry, secondRedisSetEntry);

        redisClient.prepare(messages);

        verify(redisTTL).setTtl(jedisPipeline, firstRedisSetEntry.getKey());
        verify(redisTTL).setTtl(jedisPipeline, secondRedisSetEntry.getKey());
    }

    @Test
    public void shouldCompleteTransactionInExec() {
        populateRedisDataEntry(firstRedisListEntry, secondRedisListEntry);
        when(jedisPipeline.exec()).thenReturn(responses);
        when(responses.get()).thenReturn(Collections.singletonList("MOCK_LIST_ITEM"));

        redisClient.prepare(messages);
        redisClient.execute();

        verify(jedisPipeline).exec();
        verify(instrumentation, times(1)).logDebug("jedis responses: {}", responses);
    }

    @Test
    public void shouldWaitForResponseInExec() {
        populateRedisDataEntry(firstRedisListEntry, secondRedisListEntry);
        when(jedisPipeline.exec()).thenReturn(responses);
        when(responses.get()).thenReturn(Collections.singletonList("MOCK_LIST_ITEM"));

        redisClient.prepare(messages);
        redisClient.execute();

        verify(jedisPipeline).sync();
    }

    @Test
    public void shouldThrowExceptionWhenResponseIsNullInExec() {
        expectedException.expect(NoResponseException.class);

        populateRedisDataEntry(firstRedisListEntry, secondRedisListEntry);
        when(jedisPipeline.exec()).thenReturn(responses);
        when(responses.get()).thenReturn(null);

        redisClient.prepare(messages);
        redisClient.execute();
    }

    @Test
    public void shouldThrowExceptionWhenResponseIsEmptyInExec() {
        expectedException.expect(NoResponseException.class);

        populateRedisDataEntry(firstRedisListEntry, secondRedisListEntry);
        when(jedisPipeline.exec()).thenReturn(responses);
        when(responses.get()).thenReturn(new ArrayList<>());

        redisClient.prepare(messages);
        redisClient.execute();
    }

    @Test
    public void shouldReturnEmptyArrayInExec() {
        populateRedisDataEntry(firstRedisListEntry, secondRedisListEntry);
        when(jedisPipeline.exec()).thenReturn(responses);
        when(responses.get()).thenReturn(Collections.singletonList("MOCK_LIST_ITEM"));

        redisClient.prepare(messages);
        List<Message> elementsToRetry = redisClient.execute();

        Assert.assertEquals(0, elementsToRetry.size());
    }

    @Test
    public void shouldCloseTheClient() {
        redisClient.close();

        verify(instrumentation, times(1)).logInfo("Closing Jedis client");
        verify(jedis, times(1)).close();
    }


    private void populateRedisDataEntry(RedisDataEntry... redisData) {
        redisDataEntries.addAll(Arrays.asList(redisData));
    }
}
