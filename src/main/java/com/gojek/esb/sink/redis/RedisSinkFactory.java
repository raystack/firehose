package com.gojek.esb.sink.redis;


import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.AbstractSink;
import com.gojek.esb.sink.SinkFactory;
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
        RedisClientFactory redisClientFactory = new RedisClientFactory(redisSinkConfig, stencilClient);
        return new RedisSink(new Instrumentation(statsDReporter, RedisSink.class), "redis", redisClientFactory.getClient());
    }
}
