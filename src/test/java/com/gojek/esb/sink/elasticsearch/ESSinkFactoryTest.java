package com.gojek.esb.sink.elasticsearch;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.Sink;
import org.apache.http.HttpHost;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ESSinkFactoryTest {

    private Map<String, String> configuration;

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private StencilClient stencilClient;

    @Before
    public void setUp() {
        configuration = new HashMap<>();
    }

    @Test
    public void shouldCreateESSink() {
        configuration.put("ES_CONNECTION_URLS", "localhost:9200 , localhost:9200 ");
        ESSinkFactory esSinkFactory = new ESSinkFactory();
        Sink sink = esSinkFactory.create(configuration, statsDReporter, stencilClient);
        assertEquals(ESSink.class, sink.getClass());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForEmptyESConnectionURLs() {
        ESSinkFactory esSinkFactory = new ESSinkFactory();
        try {
            esSinkFactory.getHttpHosts("");
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("ES_CONNECTION_URLS is empty or null", e.getMessage());
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForNullESConnectionURLs() {
        ESSinkFactory esSinkFactory = new ESSinkFactory();
        try {
            esSinkFactory.getHttpHosts(null);
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("ES_CONNECTION_URLS is empty or null", e.getMessage());
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForEmptyHostName() {
        ESSinkFactory esSinkFactory = new ESSinkFactory();
        String esConnectionURLs = ":1000";
        try {
            esSinkFactory.getHttpHosts(esConnectionURLs);
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
        }
    }

    @Test
    public void shouldGetHttpHostsForValidESConnectionURLs() {
        ESSinkFactory esSinkFactory = new ESSinkFactory();
        String esConnectionURLs = "localhost_1:1000,localhost_2:1000";
        HttpHost[] httpHosts = esSinkFactory.getHttpHosts(esConnectionURLs);

        assertEquals("localhost_1", httpHosts[0].getHostName());
        assertEquals(1000, httpHosts[0].getPort());
        assertEquals("localhost_2", httpHosts[1].getHostName());
        assertEquals(1000, httpHosts[1].getPort());
    }

    @Test
    public void shouldGetHttpHostsForValidESConnectionURLsWithSpacesInBetween() {
        ESSinkFactory esSinkFactory = new ESSinkFactory();
        String esConnectionURLs = " localhost_1: 1000,  localhost_2:1000";
        HttpHost[] httpHosts = esSinkFactory.getHttpHosts(esConnectionURLs);

        assertEquals("localhost_1", httpHosts[0].getHostName());
        assertEquals(1000, httpHosts[0].getPort());
        assertEquals("localhost_2", httpHosts[1].getHostName());
        assertEquals(1000, httpHosts[1].getPort());
    }

    @Test
    public void shouldGetHttpHostsForIPInESConnectionURLs() {
        ESSinkFactory esSinkFactory = new ESSinkFactory();
        String esConnectionURLs = "172.28.32.156:1000";
        HttpHost[] httpHosts = esSinkFactory.getHttpHosts(esConnectionURLs);

        assertEquals("172.28.32.156", httpHosts[0].getHostName());
        assertEquals(1000, httpHosts[0].getPort());
    }

}
