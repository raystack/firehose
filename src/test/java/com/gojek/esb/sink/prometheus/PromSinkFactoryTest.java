package com.gojek.esb.sink.prometheus;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.AbstractSink;
import org.gradle.internal.impldep.org.junit.Before;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockserver.integration.ClientAndServer;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;

@RunWith(MockitoJUnitRunner.class)
public class PromSinkFactoryTest {
    @Mock
    private StatsDReporter statsDReporter;
    @Mock
    private StencilClient stencilClient;

    private static ClientAndServer mockServer;

    @Before
    public void setup() {
        initMocks(this);
    }

    @BeforeClass
    public static void startServer() {
        mockServer = startClientAndServer(1080);
    }

    @AfterClass
    public static void stopServer() {
        mockServer.stop();
    }

    @Test
    public void shouldCreatePromSink() throws DeserializerException {

        Map<String, String> configuration = new HashMap<>();
        configuration.put("sink.prom.service.url", "http://127.0.0.1:1080/api");
        AbstractSink sink = new PromSinkFactory().create(configuration, statsDReporter, stencilClient);

        assertEquals(PromSink.class, sink.getClass());
    }
}
