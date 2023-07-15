package org.raystack.firehose.sink.prometheus;


import org.raystack.firehose.exception.DeserializerException;
import org.raystack.firehose.sink.AbstractSink;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.stencil.client.StencilClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class PromSinkFactoryTest {
    @Mock
    private StatsDReporter statsDReporter;
    @Mock
    private StencilClient stencilClient;

    @Test
    public void shouldCreatePromSink() throws DeserializerException {

        Map<String, String> configuration = new HashMap<>();
        configuration.put("SINK_PROM_SERVICE_URL", "dummyEndpoint");
        AbstractSink sink = PromSinkFactory.create(configuration, statsDReporter, stencilClient);

        assertEquals(PromSink.class, sink.getClass());
    }
}
