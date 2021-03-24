package io.odpf.firehose.sink.prometheus.request;

import com.gojek.de.stencil.parser.ProtoParser;
import io.odpf.firehose.config.PrometheusSinkConfig;
import io.odpf.firehose.metrics.StatsDReporter;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.mockito.MockitoAnnotations.initMocks;

public class PromRequestCreatorTest {

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private ProtoParser protoParser;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldCreateNotNullRequest() {

        Properties promConfigProps = new Properties();

        PrometheusSinkConfig prometheusSinkConfig = ConfigFactory.create(PrometheusSinkConfig.class, promConfigProps);
        PromRequestCreator promRequestCreator = new PromRequestCreator(statsDReporter, prometheusSinkConfig, protoParser);

        PromRequest request = promRequestCreator.createRequest();

        assertNotNull(request);
    }
}
