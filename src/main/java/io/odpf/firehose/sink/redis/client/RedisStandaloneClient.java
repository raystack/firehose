package io.odpf.firehose.sink.redis.client;

import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.firehose.sink.redis.dataentry.RedisDataEntry;
import io.odpf.firehose.sink.redis.exception.NoResponseException;
import io.odpf.firehose.sink.redis.parsers.RedisParser;
import io.odpf.firehose.sink.redis.ttl.RedisTtl;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.List;

/**
 * Redis standalone client.
 */
public class RedisStandaloneClient implements RedisClient {

    private FirehoseInstrumentation firehoseInstrumentation;
    private RedisParser redisParser;
    private RedisTtl redisTTL;
    private Jedis jedis;
    private Pipeline jedisPipelined;

    /**
     * Instantiates a new Redis standalone client.
     *
     * @param firehoseInstrumentation the instrumentation
     * @param redisParser     the redis parser
     * @param redisTTL        the redis ttl
     * @param jedis           the jedis
     */
    public RedisStandaloneClient(FirehoseInstrumentation firehoseInstrumentation, RedisParser redisParser, RedisTtl redisTTL, Jedis jedis) {
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.redisParser = redisParser;
        this.redisTTL = redisTTL;
        this.jedis = jedis;
    }

    @Override
    public void prepare(List<Message> messages) {
        List<RedisDataEntry> redisDataEntries = redisParser.parse(messages);
        jedisPipelined = jedis.pipelined();

        jedisPipelined.multi();
        redisDataEntries.forEach(redisDataEntry -> redisDataEntry.pushMessage(jedisPipelined, redisTTL));
    }

    @Override
    public List<Message> execute() {
        Response<List<Object>> responses = jedisPipelined.exec();
        firehoseInstrumentation.logDebug("jedis responses: {}", responses);
        jedisPipelined.sync();
        if (responses.get() == null || responses.get().isEmpty()) {
            throw new NoResponseException();
        }
        return new ArrayList<>();
    }

    @Override
    public void close() {
        firehoseInstrumentation.logInfo("Closing Jedis client");
        jedis.close();
    }
}
