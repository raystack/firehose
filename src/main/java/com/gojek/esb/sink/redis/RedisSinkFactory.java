package com.gojek.esb.sink.redis;


import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.sink.SinkFactory;
import com.gojek.esb.sink.redis.list.RedisListMessageParser;
import com.gojek.esb.sink.redis.list.RedisListSink;
import org.aeonbits.owner.ConfigFactory;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * Factory class to create the RedisSink.
 * <p>
 * The firehose would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDClient statsDReporter, StencilClient client)}
 * to obtain the RedisSink implementation.
 */
public class RedisSinkFactory implements SinkFactory {

    public Sink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient client) {
        RedisSinkConfig redisSinkConfig = ConfigFactory.create(RedisSinkConfig.class, configuration);
        Jedis jedis = new Jedis(redisSinkConfig.getRedisHost(), redisSinkConfig.getRedisPort());
        RedisClient redisClient = new RedisClient(jedis);
        ProtoParser protoParser = new ProtoParser(client, redisSinkConfig.getProtoSchema());

        if (redisSinkConfig.getRedisSinkType().equalsIgnoreCase("hash_set")) {
            RedisMessageParser redisHashSetMessageParser = getRedisMessageParser(client, protoParser, redisSinkConfig);
            return new RedisSink(redisClient, redisHashSetMessageParser, statsDReporter);
        }
        RedisListMessageParser redisListMessageParser = new RedisListMessageParser(protoParser, redisSinkConfig);
        return new RedisListSink(redisClient, redisListMessageParser, statsDReporter);
    }

    private RedisMessageParser getRedisMessageParser(StencilClient client, ProtoParser protoParser, RedisSinkConfig redisSinkConfig) {
        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(protoParser, redisSinkConfig.getProtoToFieldMapping());
        return new RedisMessageParser(protoToFieldMapper, protoParser, redisSinkConfig);
    }
}
