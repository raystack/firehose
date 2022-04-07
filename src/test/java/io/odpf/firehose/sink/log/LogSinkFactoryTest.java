package io.odpf.firehose.sink.log;


import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.Sink;
import io.odpf.stencil.client.StencilClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class LogSinkFactoryTest {
    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private StencilClient stencilClient;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldCreateLogSinkForProtoBufByDefault() {
        Sink sink = LogSinkFactory.create(Collections.emptyMap(), statsDReporter, stencilClient);
        assertEquals(LogSink.class, sink.getClass());
    }

    @Test
    public void shouldCreateProtobufLogSink() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("INPUT_SCHEMA_DATA_TYPE", "protobuf");
        Sink sink = LogSinkFactory.create(configuration, statsDReporter, stencilClient);
        assertEquals(LogSink.class, sink.getClass());
    }

    @Test
    public void shouldCreateLogSinkForJson() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("INPUT_SCHEMA_DATA_TYPE", "json");
        Sink sink = LogSinkFactory.create(configuration, statsDReporter, stencilClient);
        assertEquals(LogSinkforJson.class, sink.getClass());
    }
}
