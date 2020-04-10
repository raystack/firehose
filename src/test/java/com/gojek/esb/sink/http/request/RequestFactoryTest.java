package com.gojek.esb.sink.http.request;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.sink.http.request.uri.UriParser;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class RequestFactoryTest {
    @Mock
    private StencilClient stencilClient;
    @Mock
    private UriParser uriParser;

    private Map<String, String> configuration;

    @Before
    public void setup() {
        configuration = new HashMap<String, String>();
    }

    @Test
    public void shouldReturnBatchRequestWhenPrameterSourceIsDisabledAndServiceUrlIsNotParametrised() {
        configuration.put("SERVICE_URL", "http://dummyurl.com/");

        Request request = new RequestFactory(configuration, stencilClient, uriParser).create();

        assertTrue(request instanceof BatchRequest);
    }

    @Test
    public void shouldReturnDynamicUrlRequestWhenPrameterSourceIsDisabledAndServiceUrlIsNotParametrised() {
        configuration.put("SERVICE_URL", "http://dummyurl.com/%%s,6");

        Request request = new RequestFactory(configuration, stencilClient, uriParser).create();

        assertTrue(request instanceof DynamicUrlRequest);
    }

    @Test
    public void shouldReturnParameterizedRequstWhenParameterSourceIsNotDisableAndPlacementTypeIsHeader() {
        configuration.put("HTTP_SINK_PARAMETER_SOURCE", "key");
        configuration.put("HTTP_SINK_PARAMETER_PLACEMENT", "header");

        Request request = new RequestFactory(configuration, stencilClient, uriParser).create();

        assertTrue(request instanceof ParameterizedRequest);
    }

    @Test
    public void shouldReturnParameterizedRequstWhenParameterSourceIsNotDisableAndPlacementTypeIsQuery() {
        configuration.put("HTTP_SINK_PARAMETER_SOURCE", "key");
        configuration.put("HTTP_SINK_PARAMETER_PLACEMENT", "query");

        Request request = new RequestFactory(configuration, stencilClient, uriParser).create();

        assertTrue(request instanceof ParameterizedRequest);
    }
}
