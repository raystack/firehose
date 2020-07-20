package com.gojek.esb.sink.http.request.types;

import com.gojek.esb.config.HTTPSinkConfig;
import com.gojek.esb.config.enums.HttpRequestMethod;
import com.gojek.esb.config.enums.HttpSinkDataFormat;
import com.gojek.esb.config.enums.HttpSinkParameterSourceType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.entity.EntityBuilder;
import com.gojek.esb.sink.http.request.header.HeaderBuilder;
import com.gojek.esb.sink.http.request.uri.URIBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import static org.gradle.internal.impldep.org.junit.Assert.assertFalse;
import static org.gradle.internal.impldep.org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class BatchRequestTest {

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

    private BatchRequest batchRequest;
    private HttpRequestMethod httpRequestMethod;

    @Before
    public void setup() {
        initMocks(this);
        httpRequestMethod = HttpRequestMethod.POST;
        when(httpSinkConfig.getServiceURL()).thenReturn("http://127.0.0.1:1080/api");
    }

    @Test
    public void shouldProcessBaseCase() {
        when(httpSinkConfig.getHttpSinkParameterSource()).thenReturn(HttpSinkParameterSourceType.DISABLED);

        batchRequest = new BatchRequest(httpSinkConfig, jsonBody, httpRequestMethod);
        boolean canProcess = batchRequest.canProcess();
        assertTrue(canProcess);
    }

    @Test
    public void shouldNotProcessForDyanamicURL() {
        when(httpSinkConfig.getServiceURL()).thenReturn("http://127.0.0.1:1080/api,%s");

        batchRequest = new BatchRequest(httpSinkConfig, jsonBody, httpRequestMethod);
        boolean canProcess = batchRequest.canProcess();

        assertFalse(canProcess);
    }

    @Test
    public void shouldNotProcessIfParameterIsEnabled() {
        when(httpSinkConfig.getHttpSinkParameterSource()).thenReturn(HttpSinkParameterSourceType.MESSAGE);

        batchRequest = new BatchRequest(httpSinkConfig, jsonBody, httpRequestMethod);
        boolean canProcess = batchRequest.canProcess();

        assertFalse(canProcess);
    }

    @Test
    public void shouldNotProcessTemplatesIfAbsent() {
        batchRequest = new BatchRequest(httpSinkConfig, jsonBody, httpRequestMethod);
        boolean isTemplate = batchRequest.isTemplateBody(httpSinkConfig);

        assertFalse(isTemplate);
    }

    @Test
    public void shouldProcessTemplatesIfPresent() {
        when(httpSinkConfig.getHttpSinkDataFormat()).thenReturn(HttpSinkDataFormat.JSON);
        when(httpSinkConfig.getHttpSinkJsonBodyTemplate()).thenReturn("{\"test\":\"$.routes[0]\", \"$.order_number\" : \"xxx\"}");

        batchRequest = new BatchRequest(httpSinkConfig, jsonBody, httpRequestMethod);
        boolean isTemplate = batchRequest.isTemplateBody(httpSinkConfig);

        assertTrue(isTemplate);
    }

    @Test
    public void shouldCheckTemplateAvailabilityForSettingRequestStrategy() {
        when(httpSinkConfig.getHttpSinkDataFormat()).thenReturn(HttpSinkDataFormat.JSON);
        when(httpSinkConfig.getHttpSinkJsonBodyTemplate()).thenReturn("{\"test\":\"$.routes[0]\", \"$.order_number\" : \"xxx\"}");

        batchRequest = new BatchRequest(httpSinkConfig, jsonBody, httpRequestMethod);
        batchRequest.setRequestStrategy(headerBuilder, uriBuilder, entityBuilder);

        verify(httpSinkConfig, times(1)).getHttpSinkDataFormat();
        verify(httpSinkConfig, times(1)).getHttpSinkJsonBodyTemplate();
    }

    @org.junit.Test
    public void shouldProcessMessagesInBatchIfTemplateDisabled() throws URISyntaxException {
        List<String> serializedMessages = Arrays.asList("Hello", "World!", "How");
        List<EsbMessage> messages = Arrays.asList(esbMessage, esbMessage, esbMessage);
        when(httpSinkConfig.getHttpSinkDataFormat()).thenReturn(HttpSinkDataFormat.PROTO);
        when(jsonBody.serialize(any())).thenReturn(serializedMessages);
        when(entityBuilder.setWrapping(true)).thenReturn(entityBuilder);

        batchRequest = new BatchRequest(httpSinkConfig, jsonBody, httpRequestMethod);
        Request request = batchRequest.setRequestStrategy(headerBuilder, uriBuilder, entityBuilder);
        request.build(messages);

        verify(uriBuilder, times(1)).build();
        verify(headerBuilder, times(1)).build();
        verify(entityBuilder, times(1)).buildHttpEntity(any(String.class));
    }

    @Test
    public void shouldProcessMessagesIndividuallyIfTemplateEnabled() throws URISyntaxException {
        List<String> serializedMessages = Arrays.asList("Hello", "World!", "How");
        List<EsbMessage> messages = Arrays.asList(esbMessage, esbMessage, esbMessage);
        when(httpSinkConfig.getHttpSinkDataFormat()).thenReturn(HttpSinkDataFormat.JSON);
        when(httpSinkConfig.getHttpSinkJsonBodyTemplate()).thenReturn("{\"test\":\"$.routes[0]\", \"$.order_number\" : \"xxx\"}");
        when(jsonBody.serialize(any())).thenReturn(serializedMessages);
        when(entityBuilder.setWrapping(false)).thenReturn(entityBuilder);

        batchRequest = new BatchRequest(httpSinkConfig, jsonBody, httpRequestMethod);
        Request request = batchRequest.setRequestStrategy(headerBuilder, uriBuilder, entityBuilder);
        request.build(messages);

        verify(uriBuilder, times(3)).build(esbMessage);
        verify(headerBuilder, times(3)).build(esbMessage);
        verify(entityBuilder, times(3)).buildHttpEntity(any(String.class));
    }
}
