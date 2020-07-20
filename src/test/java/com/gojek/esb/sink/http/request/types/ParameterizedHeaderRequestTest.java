package com.gojek.esb.sink.http.request.types;

import com.gojek.esb.config.HTTPSinkConfig;
import com.gojek.esb.config.enums.HttpRequestMethod;
import com.gojek.esb.config.enums.HttpSinkDataFormat;
import com.gojek.esb.config.enums.HttpSinkParameterPlacementType;
import com.gojek.esb.config.enums.HttpSinkParameterSourceType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.entity.EntityBuilder;
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

public class ParameterizedHeaderRequestTest {

    @Mock
    private URIBuilder uriBuilder;

    @Mock
    private HeaderBuilder headerBuilder;

    @Mock
    private EntityBuilder entityBuilder;

    @Mock
    private JsonBody jsonBody;

    @Mock
    private HTTPSinkConfig httpSinkConfig;

    @Mock
    private EsbMessage esbMessage;

    @Mock
    private ProtoToFieldMapper protoToFieldMapper;

    private ParameterizedHeaderRequest parameterizedHeaderRequest;
    private HttpRequestMethod httpRequestMethod;

    @Before
    public void setup() {
        initMocks(this);
        httpRequestMethod = HttpRequestMethod.POST;
        when(httpSinkConfig.getServiceURL()).thenReturn("http://127.0.0.1:1080/api");
    }

    @Test
    public void shouldProcessForParametrizedHeaders() {
        when(httpSinkConfig.getHttpSinkParameterSource()).thenReturn(HttpSinkParameterSourceType.MESSAGE);
        when(httpSinkConfig.getHttpSinkParameterPlacement()).thenReturn(HttpSinkParameterPlacementType.HEADER);

        parameterizedHeaderRequest = new ParameterizedHeaderRequest(httpSinkConfig, jsonBody, httpRequestMethod, protoToFieldMapper);
        boolean canProcess = parameterizedHeaderRequest.canProcess();
        assertTrue(canProcess);
    }

    @Test
    public void shouldNotProcessIfParameterPlacementDisabled() {
        when(httpSinkConfig.getHttpSinkParameterSource()).thenReturn(HttpSinkParameterSourceType.DISABLED);

        parameterizedHeaderRequest = new ParameterizedHeaderRequest(httpSinkConfig, jsonBody, httpRequestMethod, protoToFieldMapper);
        boolean canProcess = parameterizedHeaderRequest.canProcess();

        assertFalse(canProcess);
    }

    @Test
    public void shouldNotProcessIfParameterPlacedInQuery() {
        when(httpSinkConfig.getHttpSinkParameterSource()).thenReturn(HttpSinkParameterSourceType.MESSAGE);
        when(httpSinkConfig.getHttpSinkParameterPlacement()).thenReturn(HttpSinkParameterPlacementType.QUERY);

        parameterizedHeaderRequest = new ParameterizedHeaderRequest(httpSinkConfig, jsonBody, httpRequestMethod, protoToFieldMapper);
        boolean canProcess = parameterizedHeaderRequest.canProcess();

        assertFalse(canProcess);
    }

    @Test
    public void shouldNotProcessTemplatesIfAbsent() {
        parameterizedHeaderRequest = new ParameterizedHeaderRequest(httpSinkConfig, jsonBody, httpRequestMethod, protoToFieldMapper);
        boolean isTemplate = parameterizedHeaderRequest.isTemplateBody(httpSinkConfig);

        assertFalse(isTemplate);
    }

    @Test
    public void shouldProcessTemplatesIfPresent() {
        when(httpSinkConfig.getHttpSinkDataFormat()).thenReturn(HttpSinkDataFormat.JSON);
        when(httpSinkConfig.getHttpSinkJsonBodyTemplate()).thenReturn("{\"test\":\"$.routes[0]\", \"$.order_number\" : \"xxx\"}");

        parameterizedHeaderRequest = new ParameterizedHeaderRequest(httpSinkConfig, jsonBody, httpRequestMethod, protoToFieldMapper);
        boolean isTemplate = parameterizedHeaderRequest.isTemplateBody(httpSinkConfig);

        assertTrue(isTemplate);
    }

    @org.junit.Test
    public void shouldCheckForTemplateWhileBuilding() throws URISyntaxException {
        when(httpSinkConfig.getHttpSinkDataFormat()).thenReturn(HttpSinkDataFormat.JSON);
        when(httpSinkConfig.getHttpSinkParameterSource()).thenReturn(HttpSinkParameterSourceType.MESSAGE);
        when(httpSinkConfig.getHttpSinkDataFormat()).thenReturn(HttpSinkDataFormat.JSON);
        when(httpSinkConfig.getHttpSinkJsonBodyTemplate()).thenReturn("{\"test\":\"$.routes[0]\", \"$.order_number\" : \"xxx\"}");
        when(jsonBody.serialize(any())).thenReturn(Collections.singletonList("test"));
        when(entityBuilder.setWrapping(false)).thenReturn(entityBuilder);
        when(headerBuilder.withParameterizedHeader(protoToFieldMapper, HttpSinkParameterSourceType.MESSAGE)).thenReturn(headerBuilder);

        parameterizedHeaderRequest = new ParameterizedHeaderRequest(httpSinkConfig, jsonBody, httpRequestMethod, protoToFieldMapper);
        Request request = parameterizedHeaderRequest.setRequestStrategy(headerBuilder, uriBuilder, entityBuilder);
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
        when(entityBuilder.setWrapping(false)).thenReturn(entityBuilder);
        when(headerBuilder.withParameterizedHeader(protoToFieldMapper, HttpSinkParameterSourceType.MESSAGE)).thenReturn(headerBuilder);

        parameterizedHeaderRequest = new ParameterizedHeaderRequest(httpSinkConfig, jsonBody, httpRequestMethod, protoToFieldMapper);
        Request request = parameterizedHeaderRequest
                .setRequestStrategy(headerBuilder, uriBuilder, entityBuilder);
        request.build(messages);

        verify(uriBuilder, times(3)).build(esbMessage);
        verify(headerBuilder, times(3)).build(esbMessage);
        verify(entityBuilder, times(3)).buildHttpEntity(any(String.class));
    }
}
