package com.gojek.esb.sink.http.request.types;

import com.gojek.esb.config.HTTPSinkConfig;
import com.gojek.esb.config.enums.HttpRequestMethod;
import com.gojek.esb.config.enums.HttpSinkDataFormat;
import com.gojek.esb.config.enums.HttpSinkParameterPlacementType;
import com.gojek.esb.config.enums.HttpSinkParameterSourceType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.entity.RequestEntityBuilder;
import com.gojek.esb.sink.http.request.header.HeaderBuilder;
import com.gojek.esb.sink.http.request.uri.URIBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.gradle.internal.impldep.org.junit.Assert.assertFalse;
import static org.gradle.internal.impldep.org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ParameterizedURIRequestTest {
    @Mock
    private URIBuilder uriBuilder;

    @Mock
    private HeaderBuilder headerBuilder;

    @Mock
    private RequestEntityBuilder requestEntityBuilder;

    @Mock
    private JsonBody jsonBody;

    @Mock
    private HTTPSinkConfig httpSinkConfig;

    @Mock
    private EsbMessage esbMessage;

    @Mock
    private ProtoToFieldMapper protoToFieldMapper;

    @Mock
    private StatsDReporter statsDReporter;

    private ParameterizedURIRequest parameterizedURIRequest;
    private HttpRequestMethod httpRequestMethod;

    @Before
    public void setup() {
        initMocks(this);
        httpRequestMethod = HttpRequestMethod.POST;
        when(httpSinkConfig.getServiceURL()).thenReturn("http://127.0.0.1:1080/api");
    }

    @Test
    public void shouldProcessForParametrizedQuery() {
        when(httpSinkConfig.getHttpSinkParameterSource()).thenReturn(HttpSinkParameterSourceType.MESSAGE);
        when(httpSinkConfig.getHttpSinkParameterPlacement()).thenReturn(HttpSinkParameterPlacementType.QUERY);

        parameterizedURIRequest = new ParameterizedURIRequest(statsDReporter, httpSinkConfig, jsonBody, httpRequestMethod, protoToFieldMapper);
        boolean canProcess = parameterizedURIRequest.canProcess();
        assertTrue(canProcess);
    }

    @Test
    public void shouldNotProcessIfParameterPlacementDisabled() {
        when(httpSinkConfig.getHttpSinkParameterSource()).thenReturn(HttpSinkParameterSourceType.DISABLED);

        parameterizedURIRequest = new ParameterizedURIRequest(statsDReporter, httpSinkConfig, jsonBody, httpRequestMethod, protoToFieldMapper);
        boolean canProcess = parameterizedURIRequest.canProcess();

        assertFalse(canProcess);
    }

    @Test
    public void shouldNotProcessIfParameterPlacedInHeader() {
        when(httpSinkConfig.getHttpSinkParameterSource()).thenReturn(HttpSinkParameterSourceType.MESSAGE);
        when(httpSinkConfig.getHttpSinkParameterPlacement()).thenReturn(HttpSinkParameterPlacementType.HEADER);

        parameterizedURIRequest = new ParameterizedURIRequest(statsDReporter, httpSinkConfig, jsonBody, httpRequestMethod, protoToFieldMapper);
        boolean canProcess = parameterizedURIRequest.canProcess();

        assertFalse(canProcess);
    }

    @Test
    public void shouldNotProcessTemplatesIfAbsent() {
        parameterizedURIRequest = new ParameterizedURIRequest(statsDReporter, httpSinkConfig, jsonBody, httpRequestMethod, protoToFieldMapper);
        boolean isTemplate = parameterizedURIRequest.isTemplateBody(httpSinkConfig);

        assertFalse(isTemplate);
    }

    @Test
    public void shouldProcessTemplatesIfPresent() {
        when(httpSinkConfig.getHttpSinkDataFormat()).thenReturn(HttpSinkDataFormat.JSON);
        when(httpSinkConfig.getHttpSinkJsonBodyTemplate()).thenReturn("{\"test\":\"$.routes[0]\", \"$.order_number\" : \"xxx\"}");

        parameterizedURIRequest = new ParameterizedURIRequest(statsDReporter, httpSinkConfig, jsonBody, httpRequestMethod, protoToFieldMapper);
        boolean isTemplate = parameterizedURIRequest.isTemplateBody(httpSinkConfig);

        assertTrue(isTemplate);
    }

    @org.junit.Test
    public void shouldCheckForTemplateWhileBuilding() throws URISyntaxException {
        when(httpSinkConfig.getHttpSinkDataFormat()).thenReturn(HttpSinkDataFormat.JSON);
        when(httpSinkConfig.getHttpSinkParameterSource()).thenReturn(HttpSinkParameterSourceType.MESSAGE);
        when(httpSinkConfig.getHttpSinkDataFormat()).thenReturn(HttpSinkDataFormat.JSON);
        when(httpSinkConfig.getHttpSinkJsonBodyTemplate()).thenReturn("{\"test\":\"$.routes[0]\", \"$.order_number\" : \"xxx\"}");
        when(jsonBody.serialize(any())).thenReturn(Collections.singletonList("test"));
        when(requestEntityBuilder.setWrapping(false)).thenReturn(requestEntityBuilder);
        when(uriBuilder.withParameterizedURI(protoToFieldMapper, HttpSinkParameterSourceType.MESSAGE)).thenReturn(uriBuilder);

        parameterizedURIRequest = new ParameterizedURIRequest(statsDReporter, httpSinkConfig, jsonBody, httpRequestMethod, protoToFieldMapper);
        Request request = parameterizedURIRequest.setRequestStrategy(headerBuilder, uriBuilder, requestEntityBuilder);
        request.build(Collections.singletonList(esbMessage));

        verify(httpSinkConfig, times(1)).getHttpSinkDataFormat();
        verify(httpSinkConfig, times(1)).getHttpSinkJsonBodyTemplate();
    }

    @org.junit.Test
    public void shouldProcessMessagesIndividually() throws URISyntaxException {
        List<String> serializedMessages = Arrays.asList("Hello", "World!", "How");
        List<EsbMessage> messages = Arrays.asList(esbMessage, esbMessage, esbMessage);
        when(httpSinkConfig.getHttpSinkDataFormat()).thenReturn(HttpSinkDataFormat.JSON);
        when(httpSinkConfig.getHttpSinkParameterSource()).thenReturn(HttpSinkParameterSourceType.MESSAGE);
        when(httpSinkConfig.getHttpSinkJsonBodyTemplate()).thenReturn("{\"test\":\"$.routes[0]\", \"$.order_number\" : \"xxx\"}");
        when(jsonBody.serialize(any())).thenReturn(serializedMessages);
        when(requestEntityBuilder.setWrapping(false)).thenReturn(requestEntityBuilder);
        when(uriBuilder.withParameterizedURI(protoToFieldMapper, HttpSinkParameterSourceType.MESSAGE)).thenReturn(uriBuilder);

        parameterizedURIRequest = new ParameterizedURIRequest(statsDReporter, httpSinkConfig, jsonBody, httpRequestMethod, protoToFieldMapper);
        Request request = parameterizedURIRequest
                .setRequestStrategy(headerBuilder, uriBuilder, requestEntityBuilder);
        request.build(messages);

        verify(uriBuilder, times(3)).build(esbMessage);
        verify(headerBuilder, times(3)).build(esbMessage);
        verify(requestEntityBuilder, times(3)).buildHttpEntity(any(String.class));
    }
}
