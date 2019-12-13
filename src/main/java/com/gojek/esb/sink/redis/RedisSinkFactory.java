package com.gojek.esb.sink.redis;


import java.util.Map;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.sink.SinkFactory;
import com.gojek.esb.sink.redis.client.RedisClient;
import com.gojek.esb.sink.redis.parsers.RedisParser;
import com.gojek.esb.sink.redis.parsers.RedisParserFactory;

import org.aeonbits.owner.ConfigFactory;

import redis.clients.jedis.Jedis;

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
        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(protoParser, redisSinkConfig.getProtoToFieldMapping());

        RedisParser redisParser = RedisParserFactory.getParser(protoToFieldMapper, protoParser, redisSinkConfig, statsDReporter);
        Instrumentation instrumentation = new Instrumentation(statsDReporter);
        return new RedisSink(redisClient, redisParser, instrumentation);
    }
}
