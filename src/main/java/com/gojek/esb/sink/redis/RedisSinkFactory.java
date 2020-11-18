package com.gojek.esb.sink.redis;


import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.AbstractSink;
import com.gojek.esb.sink.SinkFactory;
import com.gojek.esb.sink.redis.client.RedisClient;
import com.gojek.esb.sink.redis.client.RedisClientFactory;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

/**
 * Factory class to create the RedisSink.
 * <p>
 * The firehose would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDClient statsDReporter, StencilClient client)}
 * to obtain the RedisSink implementation.
 */
public class RedisSinkFactory implements SinkFactory {

    public AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        RedisSinkConfig redisSinkConfig = ConfigFactory.create(RedisSinkConfig.class, configuration);
        Instrumentation instrumentation = new Instrumentation(statsDReporter, RedisSinkFactory.class);
        String redisConfig = String.format("\n\tredis.urls = %s\n\tredis.key.template = %s\n\tredis.sink.type = %s"
                        + "\n\tredis.list.data.proto.index = %s\n\tredis.ttl.type = %s\n\tredis.ttl.value = %d",
                redisSinkConfig.getRedisUrls(),
                redisSinkConfig.getRedisKeyTemplate(),
                redisSinkConfig.getRedisSinkType().toString(),
                redisSinkConfig.getRedisListDataProtoIndex(),
                redisSinkConfig.getRedisTTLType().toString(),
                redisSinkConfig.getRedisTTLValue());
        instrumentation.logDebug(redisConfig);
        instrumentation.logInfo("Redis server type = {}", redisSinkConfig.getRedisServerType());

        RedisClientFactory redisClientFactory = new RedisClientFactory(statsDReporter, redisSinkConfig, stencilClient);
        RedisClient client = redisClientFactory.getClient();
        instrumentation.logInfo("Connection to redis established successfully");
        return new RedisSink(new Instrumentation(statsDReporter, RedisSink.class), "redis", client);
    }
}
