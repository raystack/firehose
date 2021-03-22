package com.gojek.esb.sink.prometheus.request;

import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.PrometheusSinkConfig;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.http.request.uri.UriParser;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertNotNull;
import static org.mockito.MockitoAnnotations.initMocks;

public class PromRequestCreatorTest {
    @Mock
    private UriParser uriParser;

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private ProtoParser protoParser;

    @Mock
    private PrometheusSinkConfig prometheusSinkConfig;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldNotReturnNullPointerWhenCreateRequest() {
        PromRequestCreator promRequestCreator = new PromRequestCreator(statsDReporter, prometheusSinkConfig, protoParser, uriParser);

        assertNotNull(promRequestCreator.createRequest());
    }
}
