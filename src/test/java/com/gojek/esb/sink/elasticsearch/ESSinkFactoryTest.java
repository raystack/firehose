package com.gojek.esb.sink.elasticsearch;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.Sink;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class ESSinkFactoryTest {

    private ESSinkFactory esSinkFactory;

    private Map<String, String> configuration;

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private StencilClient stencilClient;

    @Before
    public void setUp() throws Exception {
        esSinkFactory = new ESSinkFactory();
        configuration = new HashMap<>();
    }

    @After
    public void tearDown() throws Exception {
        esSinkFactory = null;
    }

    @Test
    public void shouldCreateESSink() {
        configuration.put("ES_BATCH_RETRY_COUNT", "3");
        configuration.put("ES_BATCH_SIZE", "1000");
        configuration.put("ES_CONNECTION_URLS", "localhost: 9200 , localhost : 9200 ");
        Sink sink = esSinkFactory.create(configuration, statsDReporter, stencilClient);
        assertTrue(sink instanceof ESSink);
    }

    @Test
    public void shouldCreateUpdateOnlyESSink() {
        configuration.put("ES_BATCH_RETRY_COUNT", "3");
        configuration.put("ES_BATCH_SIZE", "1000");
        configuration.put("ES_CONNECTION_URLS", "localhost: 9200 , localhost : 9200 ");
        configuration.put("ES_UPDATE_ONLY_MODE", "true");
        Sink sink = esSinkFactory.create(configuration, statsDReporter, stencilClient);
        assertTrue(sink instanceof ESSink);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotBeAbleToCreateESSink() {
        esSinkFactory.create(configuration, statsDReporter, stencilClient);
        fail("Should not have reached here");
    }
}
