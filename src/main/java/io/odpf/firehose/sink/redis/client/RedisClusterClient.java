package io.odpf.firehose.sink.redis.client;

import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.firehose.sink.redis.dataentry.RedisDataEntry;
import io.odpf.firehose.sink.redis.parsers.RedisParser;
import io.odpf.firehose.sink.redis.ttl.RedisTtl;
import redis.clients.jedis.JedisCluster;

import java.util.ArrayList;
import java.util.List;

/**
 * Redis cluster client.
 */
public class RedisClusterClient implements RedisClient {

    private FirehoseInstrumentation firehoseInstrumentation;
    private RedisParser redisParser;
    private RedisTtl redisTTL;
    private JedisCluster jedisCluster;
    private List<RedisDataEntry> redisDataEntries;

    /**
     * Instantiates a new Redis cluster client.
     *
     * @param firehoseInstrumentation the instrumentation
     * @param redisParser     the redis parser
     * @param redisTTL        the redis ttl
     * @param jedisCluster    the jedis cluster
     */
    public RedisClusterClient(FirehoseInstrumentation firehoseInstrumentation, RedisParser redisParser, RedisTtl redisTTL, JedisCluster jedisCluster) {
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.redisParser = redisParser;
        this.redisTTL = redisTTL;
        this.jedisCluster = jedisCluster;
    }

    @Override
    public void prepare(List<Message> messages) {
        redisDataEntries = redisParser.parse(messages);
    }

    @Override
    public List<Message> execute() {
        redisDataEntries.forEach(redisDataEntry -> redisDataEntry.pushMessage(jedisCluster, redisTTL));
        return new ArrayList<>();
    }

    @Override
    public void close() {
        firehoseInstrumentation.logInfo("Closing Jedis client");
        jedisCluster.close();
    }
}
