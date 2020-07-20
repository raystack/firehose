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
import org.gradle.internal.impldep.org.junit.Assert;
import org.junit.Before;
import org.mockito.Mock;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.gradle.internal.impldep.org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class DynamicUrlRequestTest {
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

    private DynamicUrlRequest dynamicUrlRequest;
    private HttpRequestMethod httpRequestMethod;

    @Before
    public void setup() {
        initMocks(this);
        httpRequestMethod = HttpRequestMethod.POST;

        when(httpSinkConfig.getServiceURL()).thenReturn("http://127.0.0.1:1080/api,%s");
    }

    @org.junit.Test
    public void shouldProcessForDynamicURI() {
        when(httpSinkConfig.getHttpSinkParameterSource()).thenReturn(HttpSinkParameterSourceType.DISABLED);

        dynamicUrlRequest = new DynamicUrlRequest(httpSinkConfig, jsonBody, httpRequestMethod);
        boolean canProcess = dynamicUrlRequest.canProcess();
        Assert.assertTrue(canProcess);
    }

    @org.junit.Test
    public void shouldNotProcessForBaseCase() {
        dynamicUrlRequest = new DynamicUrlRequest(httpSinkConfig, jsonBody, httpRequestMethod);
        boolean canProcess = dynamicUrlRequest.canProcess();

        assertFalse(canProcess);
    }

    @org.junit.Test
    public void shouldNotProcessIfParameterIsEnabled() {
        when(httpSinkConfig.getHttpSinkParameterSource()).thenReturn(HttpSinkParameterSourceType.MESSAGE);

        dynamicUrlRequest = new DynamicUrlRequest(httpSinkConfig, jsonBody, httpRequestMethod);
        boolean canProcess = dynamicUrlRequest.canProcess();

        assertFalse(canProcess);
    }

    @org.junit.Test
    public void shouldNotProcessTemplatesIfAbsent() {
        dynamicUrlRequest = new DynamicUrlRequest(httpSinkConfig, jsonBody, httpRequestMethod);
        boolean isTemplate = dynamicUrlRequest.isTemplateBody(httpSinkConfig);

        assertFalse(isTemplate);
    }

    @org.junit.Test
    public void shouldProcessTemplatesIfPresent() {
        when(httpSinkConfig.getHttpSinkDataFormat()).thenReturn(HttpSinkDataFormat.JSON);
        when(httpSinkConfig.getHttpSinkJsonBodyTemplate()).thenReturn("{\"test\":\"$.routes[0]\", \"$.order_number\" : \"xxx\"}");

        dynamicUrlRequest = new DynamicUrlRequest(httpSinkConfig, jsonBody, httpRequestMethod);
        boolean isTemplate = dynamicUrlRequest.isTemplateBody(httpSinkConfig);

        Assert.assertTrue(isTemplate);
    }

    @org.junit.Test
    public void shouldCheckForTemplateWhileBuilding() throws URISyntaxException {
        when(httpSinkConfig.getHttpSinkDataFormat()).thenReturn(HttpSinkDataFormat.JSON);
        when(httpSinkConfig.getHttpSinkJsonBodyTemplate()).thenReturn("{\"test\":\"$.routes[0]\", \"$.order_number\" : \"xxx\"}");
        when(jsonBody.serialize(any())).thenReturn(Collections.singletonList("test"));
        when(entityBuilder.setWrapping(false)).thenReturn(entityBuilder);

        dynamicUrlRequest = new DynamicUrlRequest(httpSinkConfig, jsonBody, httpRequestMethod);
        Request request = dynamicUrlRequest.setRequestStrategy(headerBuilder, uriBuilder, entityBuilder);
        request.build(Collections.singletonList(esbMessage));

        verify(httpSinkConfig, times(1)).getHttpSinkDataFormat();
        verify(httpSinkConfig, times(1)).getHttpSinkJsonBodyTemplate();
    }

    @org.junit.Test
    public void shouldProcessMessagesIndividually() throws URISyntaxException {
        List<String> serializedMessages = Arrays.asList("Hello", "World!", "How");
        List<EsbMessage> messages = Arrays.asList(esbMessage, esbMessage, esbMessage);
        when(httpSinkConfig.getHttpSinkDataFormat()).thenReturn(HttpSinkDataFormat.JSON);
        when(httpSinkConfig.getHttpSinkJsonBodyTemplate()).thenReturn("{\"test\":\"$.routes[0]\", \"$.order_number\" : \"xxx\"}");
        when(jsonBody.serialize(any())).thenReturn(serializedMessages);
        when(entityBuilder.setWrapping(false)).thenReturn(entityBuilder);

        dynamicUrlRequest = new DynamicUrlRequest(httpSinkConfig, jsonBody, httpRequestMethod);
        Request request = dynamicUrlRequest.setRequestStrategy(headerBuilder, uriBuilder, entityBuilder);
        request.build(messages);

        verify(uriBuilder, times(3)).build(esbMessage);
        verify(headerBuilder, times(3)).build(esbMessage);
        verify(entityBuilder, times(3)).buildHttpEntity(any(String.class));
    }
}
