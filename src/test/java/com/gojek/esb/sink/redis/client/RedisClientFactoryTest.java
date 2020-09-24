package com.gojek.esb.sink.redis.client;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.config.enums.RedisServerType;
import com.gojek.esb.config.enums.RedisSinkType;
import com.gojek.esb.config.enums.RedisTTLType;
import com.gojek.esb.exception.EglcConfigurationException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisClientFactoryTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private RedisSinkConfig redisSinkConfig;

    @Mock
    private StencilClient stencilClient;

    @Test
    public void shouldGetStandaloneClient() {
        when(redisSinkConfig.getRedisSinkType()).thenReturn(RedisSinkType.LIST);
        when(redisSinkConfig.getRedisTTLType()).thenReturn(RedisTTLType.DURATION);
        when(redisSinkConfig.getRedisServerType()).thenReturn(RedisServerType.STANDALONE);
        when(redisSinkConfig.getRedisUrls()).thenReturn("0.0.0.0:0");

        RedisClientFactory redisClientFactory = new RedisClientFactory(redisSinkConfig, stencilClient);

        RedisClient client = redisClientFactory.getClient();

        Assert.assertEquals(RedisStandaloneClient.class, client.getClass());
    }

    @Test
    public void shouldGetStandaloneClientWhenURLHasSpaces() {
        when(redisSinkConfig.getRedisSinkType()).thenReturn(RedisSinkType.LIST);
        when(redisSinkConfig.getRedisTTLType()).thenReturn(RedisTTLType.DURATION);
        when(redisSinkConfig.getRedisServerType()).thenReturn(RedisServerType.STANDALONE);
        when(redisSinkConfig.getRedisUrls()).thenReturn(" 0.0.0.0:0 ");
        RedisClientFactory redisClientFactory = new RedisClientFactory(redisSinkConfig, stencilClient);

        RedisClient client = redisClientFactory.getClient();

        Assert.assertEquals(RedisStandaloneClient.class, client.getClass());
    }

    @Test
    public void shouldGetClusterClient() {
        when(redisSinkConfig.getRedisSinkType()).thenReturn(RedisSinkType.LIST);
        when(redisSinkConfig.getRedisTTLType()).thenReturn(RedisTTLType.DURATION);
        when(redisSinkConfig.getRedisServerType()).thenReturn(RedisServerType.CLUSTER);
        when(redisSinkConfig.getRedisUrls()).thenReturn("0.0.0.0:0, 1.1.1.1:1");
        RedisClientFactory redisClientFactory = new RedisClientFactory(redisSinkConfig, stencilClient);

        RedisClient client = redisClientFactory.getClient();

        Assert.assertEquals(RedisClusterClient.class, client.getClass());
    }

    @Test
    public void shouldGetClusterClientWhenURLHasSpaces() {
        when(redisSinkConfig.getRedisSinkType()).thenReturn(RedisSinkType.LIST);
        when(redisSinkConfig.getRedisTTLType()).thenReturn(RedisTTLType.DURATION);
        when(redisSinkConfig.getRedisServerType()).thenReturn(RedisServerType.CLUSTER);
        when(redisSinkConfig.getRedisUrls()).thenReturn(" 0.0.0.0:0, 1.1.1.1:1 ");
        RedisClientFactory redisClientFactory = new RedisClientFactory(redisSinkConfig, stencilClient);

        RedisClient client = redisClientFactory.getClient();

        Assert.assertEquals(RedisClusterClient.class, client.getClass());
    }

    @Test
    public void shouldThrowExceptionWhenUrlIsInvalidForCluster() {
        expectedException.expect(EglcConfigurationException.class);
        expectedException.expectMessage("Invalid url(s) for redis cluster: localhost:6379,localhost:6378,localhost");

        when(redisSinkConfig.getRedisSinkType()).thenReturn(RedisSinkType.LIST);
        when(redisSinkConfig.getRedisTTLType()).thenReturn(RedisTTLType.DURATION);
        when(redisSinkConfig.getRedisServerType()).thenReturn(RedisServerType.CLUSTER);
        when(redisSinkConfig.getRedisUrls()).thenReturn("localhost:6379,localhost:6378,localhost");

        RedisClientFactory redisClientFactory = new RedisClientFactory(redisSinkConfig, stencilClient);

        redisClientFactory.getClient();
    }

    @Test
    public void shouldThrowExceptionWhenUrlIsInvalidForStandalone() {
        expectedException.expect(EglcConfigurationException.class);
        expectedException.expectMessage("Invalid url for redis standalone: localhost");

        when(redisSinkConfig.getRedisSinkType()).thenReturn(RedisSinkType.LIST);
        when(redisSinkConfig.getRedisTTLType()).thenReturn(RedisTTLType.DURATION);
        when(redisSinkConfig.getRedisServerType()).thenReturn(RedisServerType.STANDALONE);
        when(redisSinkConfig.getRedisUrls()).thenReturn("localhost");

        RedisClientFactory redisClientFactory = new RedisClientFactory(redisSinkConfig, stencilClient);

        redisClientFactory.getClient();
    }
}
