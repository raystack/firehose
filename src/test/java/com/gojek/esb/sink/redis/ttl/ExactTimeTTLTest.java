package com.gojek.esb.sink.redis.ttl;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import redis.clients.jedis.Pipeline;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class ExactTimeTTLTest {

    private ExactTimeTTL exactTimeTTL;
    @Mock
    private Pipeline pipeline;

    @Before
    public void setup() {
        initMocks(this);
        exactTimeTTL = new ExactTimeTTL(10000000L);
    }

    @Test
    public void shouldSetUnixTimeStampAsTTL() {
        exactTimeTTL.setTTL(pipeline, "test-key");
        verify(pipeline, times(1)).expireAt("test-key", 10000000L);
    }
}
