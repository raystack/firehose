package com.gojek.esb.sink.redis.client;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.config.enums.RedisSinkDeploymentType;
import com.gojek.esb.exception.EglcConfigurationException;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.gojek.esb.sink.redis.parsers.RedisParser;
import com.gojek.esb.sink.redis.parsers.RedisParserFactory;
import com.gojek.esb.sink.redis.ttl.RedisTtl;
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
        ProtoParser protoParser = new ProtoParser(stencilClient, redisSinkConfig.getInputSchemaProtoClass());
        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(protoParser, redisSinkConfig.getInputSchemaProtoToColumnMapping());
        RedisParser redisParser = RedisParserFactory.getParser(protoToFieldMapper, protoParser, redisSinkConfig, statsDReporter);
        RedisSinkDeploymentType redisSinkDeploymentType = redisSinkConfig.getSinkRedisDeploymentType();
        RedisTtl redisTTL = RedisTTLFactory.getTTl(redisSinkConfig);
        return RedisSinkDeploymentType.CLUSTER.equals(redisSinkDeploymentType)
                ? getRedisClusterClient(redisParser, redisTTL)
                : getRedisStandaloneClient(redisParser, redisTTL);
    }

    private RedisStandaloneClient getRedisStandaloneClient(RedisParser redisParser, RedisTtl redisTTL) {
        Jedis jedis = null;
        try {
            jedis = new Jedis(HostAndPort.parseString(StringUtils.trim(redisSinkConfig.getSinkRedisUrls())));
        } catch (IllegalArgumentException e) {
            throw new EglcConfigurationException(String.format("Invalid url for redis standalone: %s", redisSinkConfig.getSinkRedisUrls()));
        }
        return new RedisStandaloneClient(new Instrumentation(statsDReporter, RedisStandaloneClient.class), redisParser, redisTTL, jedis);
    }

    private RedisClusterClient getRedisClusterClient(RedisParser redisParser, RedisTtl redisTTL) {
        String[] redisUrls = redisSinkConfig.getSinkRedisUrls().split(DELIMITER);
        HashSet<HostAndPort> nodes = new HashSet<>();
        try {
            for (String redisUrl : redisUrls) {
                nodes.add(HostAndPort.parseString(StringUtils.trim(redisUrl)));
            }
        } catch (IllegalArgumentException e) {
            throw new EglcConfigurationException(String.format("Invalid url(s) for redis cluster: %s", redisSinkConfig.getSinkRedisUrls()));
        }
        JedisCluster jedisCluster = new JedisCluster(nodes);
        return new RedisClusterClient(new Instrumentation(statsDReporter, RedisClusterClient.class), redisParser, redisTTL, jedisCluster);
    }
}
