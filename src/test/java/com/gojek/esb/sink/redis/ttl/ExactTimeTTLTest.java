package com.gojek.esb.sink.redis.ttl;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class ExactTimeTTLTest {

    private ExactTimeTtl exactTimeTTL;
    @Mock
    private Pipeline pipeline;

    @Mock
    private JedisCluster jedisCluster;

    @Before
    public void setup() {
        initMocks(this);
        exactTimeTTL = new ExactTimeTtl(10000000L);
    }

    @Test
    public void shouldSetUnixTimeStampAsTTLForPipeline() {
        exactTimeTTL.setTtl(pipeline, "test-key");
        verify(pipeline, times(1)).expireAt("test-key", 10000000L);
    }

    @Test
    public void shouldSetUnixTimeStampAsTTLForCluster() {
        exactTimeTTL.setTtl(jedisCluster, "test-key");
        verify(jedisCluster, times(1)).expireAt("test-key", 10000000L);
    }
}
