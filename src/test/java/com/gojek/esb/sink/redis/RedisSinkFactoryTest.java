package com.gojek.esb.sink.redis;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.AbstractSink;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RedisSinkFactoryTest {
    private Map<String, String> configuration;

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private StencilClient stencilClient;

    @Before
    public void setUp() {
        configuration = new HashMap<>();
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldCreateRedisSink() {
        configuration.put("REDIS_URLS", "localhost:6379");
        configuration.put("REDIS_KEY_TEMPLATE", "test_%%s,6");
        configuration.put("REDIS_LIST_DATA_PROTO_INDEX", "3");

        RedisSinkFactory redisSinkFactory = new RedisSinkFactory();
        AbstractSink sink = redisSinkFactory.create(configuration, statsDReporter, stencilClient);
        assertEquals(RedisSink.class, sink.getClass());
    }
}
