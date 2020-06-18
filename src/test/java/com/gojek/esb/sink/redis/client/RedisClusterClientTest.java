package com.gojek.esb.sink.redis.client;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.sink.redis.dataentry.RedisDataEntry;
import com.gojek.esb.sink.redis.dataentry.RedisHashSetFieldEntry;
import com.gojek.esb.sink.redis.dataentry.RedisListEntry;
import com.gojek.esb.sink.redis.parsers.RedisParser;
import com.gojek.esb.sink.redis.ttl.RedisTTL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import redis.clients.jedis.JedisCluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisClusterClientTest {
    private final RedisHashSetFieldEntry firstRedisSetEntry = new RedisHashSetFieldEntry("key1", "field1", "value1");
    private final RedisHashSetFieldEntry secondRedisSetEntry = new RedisHashSetFieldEntry("key2", "field2", "value2");
    private final RedisListEntry firstRedisListEntry = new RedisListEntry("key1", "value1");
    private final RedisListEntry secondRedisListEntry = new RedisListEntry("key2", "value2");
    @Mock
    private JedisCluster jedisCluster;

    @Mock
    private RedisParser redisParser;

    @Mock
    private RedisTTL redisTTL;
    private List<EsbMessage> esbMessages;
    private RedisClusterClient redisClusterClient;
    private ArrayList<RedisDataEntry> redisDataEntries;

    @Before
    public void setup() {
        esbMessages = Arrays.asList(new EsbMessage(new byte[0], new byte[0], "topic", 0, 100),
                new EsbMessage(new byte[0], new byte[0], "topic", 0, 100));

        redisClusterClient = new RedisClusterClient(redisParser, redisTTL, jedisCluster);

        redisDataEntries = new ArrayList<>();

        when(redisParser.parse(esbMessages)).thenReturn(redisDataEntries);
    }

    @Test
    public void shouldParseEsbMessagesWhenPreparing() {
        redisClusterClient.prepare(esbMessages);

        verify(redisParser).parse(esbMessages);
    }

    @Test
    public void shouldSendAllListDataWhenExecuting() {
        populateRedisDataEntry(firstRedisListEntry, secondRedisListEntry);

        redisClusterClient.prepare(esbMessages);
        redisClusterClient.execute();

        verify(jedisCluster).lpush(firstRedisListEntry.getKey(), firstRedisListEntry.getValue());
        verify(jedisCluster).lpush(secondRedisListEntry.getKey(), secondRedisListEntry.getValue());
    }

    @Test
    public void shouldSendAllSetDataWhenExecuting() {
        populateRedisDataEntry(firstRedisSetEntry, secondRedisSetEntry);

        redisClusterClient.prepare(esbMessages);
        redisClusterClient.execute();

        verify(jedisCluster).hset(firstRedisSetEntry.getKey(), firstRedisSetEntry.getField(), firstRedisListEntry.getValue());
        verify(jedisCluster).hset(secondRedisSetEntry.getKey(), secondRedisSetEntry.getField(), secondRedisListEntry.getValue());
    }

    @Test
    public void shouldReturnEmptyArrayAfterExecuting() {
        populateRedisDataEntry(firstRedisSetEntry, secondRedisSetEntry);

        redisClusterClient.prepare(esbMessages);
        List<EsbMessage> retryElements = redisClusterClient.execute();

        Assert.assertEquals(0, retryElements.size());
    }

    @Test
    public void shouldCloseTheJedisClient() {
        redisClusterClient.close();

        verify(jedisCluster).close();
    }


    private void populateRedisDataEntry(RedisDataEntry... redisData) {
        redisDataEntries.addAll(Arrays.asList(redisData));
    }
}
