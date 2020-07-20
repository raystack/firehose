package com.gojek.esb.sink.http.request;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.config.HTTPSinkConfig;
import com.gojek.esb.sink.http.request.types.BatchRequest;
import com.gojek.esb.sink.http.request.types.DynamicUrlRequest;
import com.gojek.esb.sink.http.request.types.ParameterizedHeaderRequest;
import com.gojek.esb.sink.http.request.types.ParameterizedURIRequest;
import com.gojek.esb.sink.http.request.types.Request;
import com.gojek.esb.sink.http.request.uri.UriParser;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.mockito.MockitoAnnotations.initMocks;

public class RequestFactoryTest {
    @Mock
    private StencilClient stencilClient;
    @Mock
    private UriParser uriParser;
    private HTTPSinkConfig httpSinkConfig;

    private Map<String, String> configuration = new HashMap<>();

    @Before
    public void setup() {
        initMocks(this);
        configuration = new HashMap<String, String>();
    }

    @Test
    public void shouldReturnBatchRequestWhenPrameterSourceIsDisabledAndServiceUrlIsConstant() {
        configuration.put("SERVICE_URL", "http://127.0.0.1:1080/api");
        httpSinkConfig = ConfigFactory.create(HTTPSinkConfig.class, configuration);
        Request request = new RequestFactory(httpSinkConfig, stencilClient, uriParser).createRequest();

        assertTrue(request instanceof BatchRequest);
    }

    @Test
    public void shouldReturnDynamicUrlRequestWhenPrameterSourceIsDisabledAndServiceUrlIsNotParametrised() {
        configuration.put("SERVICE_URL", "http://127.0.0.1:1080/api,%s");
        httpSinkConfig = ConfigFactory.create(HTTPSinkConfig.class, configuration);

        Request request = new RequestFactory(httpSinkConfig, stencilClient, uriParser).createRequest();

        assertTrue(request instanceof DynamicUrlRequest);
    }

    @Test
    public void shouldReturnParameterizedRequstWhenParameterSourceIsNotDisableAndPlacementTypeIsHeader() {
        configuration.put("HTTP_SINK_PARAMETER_SOURCE", "key");
        configuration.put("HTTP_SINK_PARAMETER_PLACEMENT", "header");
        configuration.put("SERVICE_URL", "http://127.0.0.1:1080/api,%s");
        httpSinkConfig = ConfigFactory.create(HTTPSinkConfig.class, configuration);

        Request request = new RequestFactory(httpSinkConfig, stencilClient, uriParser).createRequest();

        assertTrue(request instanceof ParameterizedHeaderRequest);
    }

    @Test
    public void shouldReturnParameterizedRequstWhenParameterSourceIsNotDisableAndPlacementTypeIsQuery() {
        configuration.put("HTTP_SINK_PARAMETER_SOURCE", "key");
        configuration.put("HTTP_SINK_PARAMETER_PLACEMENT", "query");
        configuration.put("SERVICE_URL", "http://127.0.0.1:1080/api,%s");
        httpSinkConfig = ConfigFactory.create(HTTPSinkConfig.class, configuration);


        Request request = new RequestFactory(httpSinkConfig, stencilClient, uriParser).createRequest();

        assertTrue(request instanceof ParameterizedURIRequest);
    }
}
