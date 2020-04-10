package com.gojek.esb.sink.http.request;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.sink.http.request.uri.UriParser;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Map;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class RequestFactoryTest {
    @Mock
    private StencilClient stencilClient;
    @Mock
    private UriParser uriParser;

    private Map<String, String> configuration;

    @Before
    public void setup() {
        initMocks(this);
        configuration = new HashMap<String, String>();
    }

    @Test
    public void shouldReturnBatchRequestWhenPrameterSourceIsDisabledAndServiceUrlIsConstant() {
        when(uriParser.isDynamicUrl(any())).thenReturn(FALSE);

        Request request = new RequestFactory(configuration, stencilClient, uriParser).create();

        assertTrue(request instanceof BatchRequest);
    }

    @Test
    public void shouldReturnDynamicUrlRequestWhenPrameterSourceIsDisabledAndServiceUrlIsNotParametrised() {
        when(uriParser.isDynamicUrl(any())).thenReturn(TRUE);

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
