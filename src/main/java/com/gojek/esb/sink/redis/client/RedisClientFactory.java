package com.gojek.esb.sink.redis.client;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.config.enums.RedisServerType;
import com.gojek.esb.exception.EglcConfigurationException;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.gojek.esb.sink.redis.parsers.RedisParser;
import com.gojek.esb.sink.redis.parsers.RedisParserFactory;
import com.gojek.esb.sink.redis.ttl.RedisTTL;
import com.gojek.esb.sink.redis.ttl.RedisTTLFactory;
import org.apache.commons.lang.StringUtils;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;

public class RedisClientFactory {

    private static final String DELIMITER = ",";
    private StatsDReporter statsDReporter;
    private RedisSinkConfig redisSinkConfig;
    private StencilClient stencilClient;

    public RedisClientFactory(StatsDReporter statsDReporter, RedisSinkConfig redisSinkConfig, StencilClient stencilClient) {
        this.statsDReporter = statsDReporter;
        this.redisSinkConfig = redisSinkConfig;
        this.stencilClient = stencilClient;
    }

    public RedisClient getClient() {
        ProtoParser protoParser = new ProtoParser(stencilClient, redisSinkConfig.getProtoSchema());
        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(protoParser, redisSinkConfig.getProtoToFieldMapping());
        RedisParser redisParser = RedisParserFactory.getParser(protoToFieldMapper, protoParser, redisSinkConfig, statsDReporter);
        RedisServerType redisServerType = redisSinkConfig.getRedisServerType();
        RedisTTL redisTTL = RedisTTLFactory.getTTl(redisSinkConfig);
        return RedisServerType.CLUSTER.equals(redisServerType)
                ? getRedisClusterClient(redisParser, redisTTL)
                : getRedisStandaloneClient(redisParser, redisTTL);
    }

    private RedisStandaloneClient getRedisStandaloneClient(RedisParser redisParser, RedisTTL redisTTL) {
        Jedis jedis = null;
        try {
            jedis = new Jedis(HostAndPort.parseString(StringUtils.trim(redisSinkConfig.getRedisUrls())));
        } catch (IllegalArgumentException e) {
            throw new EglcConfigurationException(String.format("Invalid url for redis standalone: %s", redisSinkConfig.getRedisUrls()));
        }
        return new RedisStandaloneClient(new Instrumentation(statsDReporter, RedisStandaloneClient.class), redisParser, redisTTL, jedis);
    }

    private RedisClusterClient getRedisClusterClient(RedisParser redisParser, RedisTTL redisTTL) {
        String[] redisUrls = redisSinkConfig.getRedisUrls().split(DELIMITER);
        HashSet<HostAndPort> nodes = new HashSet<>();
        try {
            for (String redisUrl : redisUrls) {
                nodes.add(HostAndPort.parseString(StringUtils.trim(redisUrl)));
            }
        } catch (IllegalArgumentException e) {
            throw new EglcConfigurationException(String.format("Invalid url(s) for redis cluster: %s", redisSinkConfig.getRedisUrls()));
        }
        JedisCluster jedisCluster = new JedisCluster(nodes);
        return new RedisClusterClient(new Instrumentation(statsDReporter, RedisClusterClient.class), redisParser, redisTTL, jedisCluster);
    }
}
