package com.gojek.esb.sink.log;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.Sink;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LogSinkFactoryTest {

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
    public void shouldCreateLogSink() {
        LogSinkFactory logSinkFactory = new LogSinkFactory();
        Sink sink = logSinkFactory.create(configuration, statsDReporter, stencilClient);
        assertEquals(LogSink.class, sink.getClass());
    }
}
