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

    private ExactTimeTTL exactTimeTTL;
    @Mock
    private Pipeline pipeline;

    @Mock
    private JedisCluster jedisCluster;

    @Before
    public void setup() {
        initMocks(this);
        exactTimeTTL = new ExactTimeTTL(10000000L);
    }

    @Test
    public void shouldSetUnixTimeStampAsTTLForPipeline() {
        exactTimeTTL.setTTL(pipeline, "test-key");
        verify(pipeline, times(1)).expireAt("test-key", 10000000L);
    }

    @Test
    public void shouldSetUnixTimeStampAsTTLForCluster() {
        exactTimeTTL.setTTL(jedisCluster, "test-key");
        verify(jedisCluster, times(1)).expireAt("test-key", 10000000L);
    }
}
