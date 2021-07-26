package io.odpf.firehose.sink.mongodb.util;

import com.mongodb.ServerAddress;
import io.odpf.firehose.metrics.Instrumentation;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class MongoSinkUtilTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private Instrumentation instrumentation;


    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForEmptyMongoConnectionURLs() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("SINK_MONGO_CONNECTION_URLS is empty or null");

        MongoSinkUtil.getServerAddresses("", instrumentation);

    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForNullMongoConnectionURLs() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("SINK_MONGO_CONNECTION_URLS is empty or null");

        MongoSinkUtil.getServerAddresses(null, instrumentation);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForEmptyHost() {
        String mongoConnectionURLs = ":1000";
        thrown.expect(IllegalArgumentException.class);
        MongoSinkUtil.getServerAddresses(mongoConnectionURLs, instrumentation);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForEmptyPort() {
        String mongoConnectionURLs = "localhost:";
        thrown.expect(IllegalArgumentException.class);

        MongoSinkUtil.getServerAddresses(mongoConnectionURLs, instrumentation);
    }

    @Test
    public void shouldGetServerAddressesForValidMongoConnectionURLs() {
        String mongoConnectionURLs = "localhost_1:1000,localhost_2:1000";
        List<ServerAddress> serverAddresses = MongoSinkUtil.getServerAddresses(mongoConnectionURLs, instrumentation);

        assertEquals("localhost_1", serverAddresses.get(0).getHost());
        assertEquals(1000, serverAddresses.get(0).getPort());
        assertEquals("localhost_2", serverAddresses.get(1).getHost());
        assertEquals(1000, serverAddresses.get(1).getPort());
    }

    @Test
    public void shouldGetServerAddressesForValidMongoConnectionURLsWithSpacesInBetween() {
        String mongoConnectionURLs = " localhost_1: 1000,  localhost_2:1000";
        List<ServerAddress> serverAddresses = MongoSinkUtil.getServerAddresses(mongoConnectionURLs, instrumentation);

        assertEquals("localhost_1", serverAddresses.get(0).getHost());
        assertEquals(1000, serverAddresses.get(0).getPort());
        assertEquals("localhost_2", serverAddresses.get(1).getHost());
        assertEquals(1000, serverAddresses.get(1).getPort());
    }

    @Test
    public void shouldGetServerAddressForIPInMongoConnectionURLs() {
        String mongoConnectionURLs = "172.28.32.156:1000";
        List<ServerAddress> serverAddresses = MongoSinkUtil.getServerAddresses(mongoConnectionURLs, instrumentation);

        assertEquals("172.28.32.156", serverAddresses.get(0).getHost());
        assertEquals(1000, serverAddresses.get(0).getPort());
    }

    @Test
    public void shouldThrowExceptionIfHostAndPortNotProvidedProperly() {
        String mongoConnectionURLs = "test";
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("SINK_MONGO_CONNECTION_URLS should contain host and port both");

        MongoSinkUtil.getServerAddresses(mongoConnectionURLs, instrumentation);
    }
}
