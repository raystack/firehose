package io.odpf.firehose.sink.mongodb;

import com.gojek.de.stencil.client.StencilClient;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.Sink;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MongoSinkFactoryTest {

    private Map<String, String> configuration;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
    public void shouldCreateMongoSink() {
        configuration.put("SINK_MONGO_CONNECTION_URLS", "localhost:9200 , localhost:9200 ");
        configuration.put("SINK_MONGO_DB_NAME", "myDb");
        configuration.put("SINK_MONGO_COLLECTION_NAME", "sampleCollection");

        MongoSinkFactory mongoSinkFactory = new MongoSinkFactory();
        Sink sink = mongoSinkFactory.create(configuration, statsDReporter, stencilClient);
        assertEquals(MongoSink.class, sink.getClass());
    }
}
