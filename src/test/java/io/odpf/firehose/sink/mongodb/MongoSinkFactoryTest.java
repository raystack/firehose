package io.odpf.firehose.sink.mongodb;

import com.gojek.de.stencil.client.StencilClient;
import com.mongodb.ServerAddress;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.Sink;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MongoSinkFactoryTest {

    private Map<String, String> configuration;

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private StencilClient stencilClient;

    @Before
    public void setUp() {
        configuration = new HashMap<>();
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldCreateMongoSink() {
        configuration.put("SINK_MONGO_CONNECTION_URLS", "localhost:9200 , localhost:9200 ");
        MongoSinkFactory mongoSinkFactory = new MongoSinkFactory();
        Sink sink = mongoSinkFactory.create(configuration, statsDReporter, stencilClient);
        assertEquals(MongoSink.class, sink.getClass());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForEmptyMongoConnectionURLs() {
        MongoSinkFactory mongoSinkFactory = new MongoSinkFactory();
        try {
            mongoSinkFactory.getServerAddresses("", instrumentation);
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("SINK_MONGO_CONNECTION_URLS is empty or null", e.getMessage());
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForNullMongoConnectionURLs() {
        MongoSinkFactory mongoSinkFactory = new MongoSinkFactory();
        try {
            mongoSinkFactory.getServerAddresses(null, instrumentation);
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("SINK_MONGO_CONNECTION_URLS is empty or null", e.getMessage());
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForEmptyHost() {
        MongoSinkFactory mongoSinkFactory = new MongoSinkFactory();
        String mongoConnectionURLs = ":1000";
        try {
            mongoSinkFactory.getServerAddresses(mongoConnectionURLs, instrumentation);
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForEmptyPort() {
        MongoSinkFactory mongoSinkFactory = new MongoSinkFactory();
        String mongoConnectionURLs = "localhost:";
        try {
            mongoSinkFactory.getServerAddresses(mongoConnectionURLs, instrumentation);
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
        }
    }

    @Test
    public void shouldGetServerAddressesForValidMongoConnectionURLs() {
        MongoSinkFactory mongoSinkFactory = new MongoSinkFactory();
        String mongoConnectionURLs = "localhost_1:1000,localhost_2:1000";
        List<ServerAddress>  serverAddresses = mongoSinkFactory.getServerAddresses(mongoConnectionURLs, instrumentation);

        assertEquals("localhost_1", serverAddresses.get(0).getHost());
        assertEquals(1000, serverAddresses.get(0).getPort());
        assertEquals("localhost_2", serverAddresses.get(1).getHost());
        assertEquals(1000, serverAddresses.get(1).getPort());
    }

    @Test
    public void shouldGetServerAddressesForValidMongoConnectionURLsWithSpacesInBetween() {
        MongoSinkFactory mongoSinkFactory = new MongoSinkFactory();
        String mongoConnectionURLs = " localhost_1: 1000,  localhost_2:1000";
        List<ServerAddress> serverAddresses = mongoSinkFactory.getServerAddresses(mongoConnectionURLs, instrumentation);

        assertEquals("localhost_1", serverAddresses.get(0).getHost());
        assertEquals(1000, serverAddresses.get(0).getPort());
        assertEquals("localhost_2", serverAddresses.get(1).getHost());
        assertEquals(1000, serverAddresses.get(1).getPort());
    }

    @Test
    public void shouldGetServerAddressForIPInMongoConnectionURLs() {
        MongoSinkFactory mongoSinkFactory = new MongoSinkFactory();
        String mongoConnectionURLs = "172.28.32.156:1000";
        List<ServerAddress> serverAddresses = mongoSinkFactory.getServerAddresses(mongoConnectionURLs, instrumentation);

        assertEquals("172.28.32.156", serverAddresses.get(0).getHost());
        assertEquals(1000, serverAddresses.get(0).getPort());
    }

    @Test
    public void shouldThrowExceptionIfHostAndPortNotProvidedProperly() {
        MongoSinkFactory mongoSinkFactory = new MongoSinkFactory();
        String mongoConnectionURLs = "test";
        try {
            mongoSinkFactory.getServerAddresses(mongoConnectionURLs, instrumentation);
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("SINK_MONGO_CONNECTION_URLS should contain host and port both", e.getMessage());
        }
    }

    @Test
    public void shouldReturnBlackListRetryStatusCodesAsList() {
        MongoSinkFactory mongoSinkFactory = new MongoSinkFactory();
        String inputRetryStatusCodeBlacklist = "404, 502";
        List<String> statusCodesAsList = mongoSinkFactory.getStatusCodesAsList(inputRetryStatusCodeBlacklist);
        assertEquals("404", statusCodesAsList.get(0));
        assertEquals("502", statusCodesAsList.get(1));
    }

    @Test
    public void shouldReturnEmptyBlackListRetryStatusCodesAsEmptyList() {
        MongoSinkFactory mongoSinkFactory = new MongoSinkFactory();
        String inputRetryStatusCodeBlacklist = "";
        List<String> statusCodesAsList = mongoSinkFactory.getStatusCodesAsList(inputRetryStatusCodeBlacklist);
        assertEquals(0, statusCodesAsList.size());
    }
}
