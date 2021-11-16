package io.odpf.firehose.sink.redis.client;

import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.redis.dataentry.RedisDataEntry;
import io.odpf.firehose.sink.redis.dataentry.RedisHashSetFieldEntry;
import io.odpf.firehose.sink.redis.dataentry.RedisListEntry;
import io.odpf.firehose.sink.redis.parsers.RedisParser;
import io.odpf.firehose.sink.redis.ttl.RedisTtl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;
import redis.clients.jedis.JedisCluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RedisClusterClientTest {
    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private Instrumentation instrumentation;

    private final RedisHashSetFieldEntry firstRedisSetEntry = new RedisHashSetFieldEntry("key1", "field1", "value1", new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class));
    private final RedisHashSetFieldEntry secondRedisSetEntry = new RedisHashSetFieldEntry("key2", "field2", "value2", new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class));
    private final RedisListEntry firstRedisListEntry = new RedisListEntry("key1", "value1", new Instrumentation(statsDReporter, RedisListEntry.class));
    private final RedisListEntry secondRedisListEntry = new RedisListEntry("key2", "value2", new Instrumentation(statsDReporter, RedisListEntry.class));
    @Mock
    private JedisCluster jedisCluster;

    @Mock
    private RedisParser redisParser;

    @Mock
    private RedisTtl redisTTL;
    private List<Message> messages;
    private RedisClusterClient redisClusterClient;
    private ArrayList<RedisDataEntry> redisDataEntries;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        messages = Arrays.asList(new Message(new byte[0], new byte[0], "topic", 0, 100),
                new Message(new byte[0], new byte[0], "topic", 0, 100));

        redisClusterClient = new RedisClusterClient(instrumentation, redisParser, redisTTL, jedisCluster);

        redisDataEntries = new ArrayList<>();

        when(redisParser.parse(messages)).thenReturn(redisDataEntries);
    }

    @Test
    public void shouldParseEsbMessagesWhenPreparing() {
        redisClusterClient.prepare(messages);

        verify(redisParser).parse(messages);
    }

    @Test
    public void shouldSendAllListDataWhenExecuting() {
        populateRedisDataEntry(firstRedisListEntry, secondRedisListEntry);

        redisClusterClient.prepare(messages);
        redisClusterClient.execute();

        verify(jedisCluster).lpush(firstRedisListEntry.getKey(), firstRedisListEntry.getValue());
        verify(jedisCluster).lpush(secondRedisListEntry.getKey(), secondRedisListEntry.getValue());
    }

    @Test
    public void shouldSendAllSetDataWhenExecuting() {
        populateRedisDataEntry(firstRedisSetEntry, secondRedisSetEntry);

        redisClusterClient.prepare(messages);
        redisClusterClient.execute();

        verify(jedisCluster).hset(firstRedisSetEntry.getKey(), firstRedisSetEntry.getField(), firstRedisListEntry.getValue());
        verify(jedisCluster).hset(secondRedisSetEntry.getKey(), secondRedisSetEntry.getField(), secondRedisListEntry.getValue());
    }

    @Test
    public void shouldReturnEmptyArrayAfterExecuting() {
        populateRedisDataEntry(firstRedisSetEntry, secondRedisSetEntry);

        redisClusterClient.prepare(messages);
        List<Message> retryElements = redisClusterClient.execute();

        Assert.assertEquals(0, retryElements.size());
    }

    @Test
    public void shouldCloseTheJedisClient() {
        redisClusterClient.close();

        verify(instrumentation, times(1)).logInfo("Closing Jedis client");
        verify(jedisCluster).close();
    }


    private void populateRedisDataEntry(RedisDataEntry... redisData) {
        redisDataEntries.addAll(Arrays.asList(redisData));
    }
}
