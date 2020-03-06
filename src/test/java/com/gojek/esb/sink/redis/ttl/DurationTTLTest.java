package com.gojek.esb.sink.redis.ttl;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import redis.clients.jedis.Pipeline;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class DurationTTLTest {

    private DurationTTL durationTTL;

    @Mock
    private Pipeline pipeline;

    @Before
    public void setup() {
        initMocks(this);
        durationTTL = new DurationTTL(10);
    }

    @Test
    public void shouldSetTTLInSeconds() {
        durationTTL.setTTL(pipeline, "test-key");
        verify(pipeline, times(1)).expire("test-key", 10);
    }
}
