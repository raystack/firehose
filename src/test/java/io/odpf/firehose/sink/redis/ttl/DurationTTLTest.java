package io.odpf.firehose.sink.redis.ttl;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class DurationTTLTest {

    private DurationTtl durationTTL;

    @Mock
    private Pipeline pipeline;

    @Mock
    private JedisCluster jedisCluster;

    @Before
    public void setup() {
        durationTTL = new DurationTtl(10);
    }

    @Test
    public void shouldSetTTLInSecondsForPipeline() {
        durationTTL.setTtl(pipeline, "test-key");
        verify(pipeline, times(1)).expire("test-key", 10);
    }

    @Test
    public void shouldSetTTLInSecondsForCluster() {
        durationTTL.setTtl(jedisCluster, "test-key");
        verify(jedisCluster, times(1)).expire("test-key", 10);
    }
}
