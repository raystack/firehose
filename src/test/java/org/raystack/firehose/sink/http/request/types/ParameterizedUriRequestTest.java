package org.raystack.firehose.sink.http.request.types;

import org.raystack.firehose.config.HttpSinkConfig;
import org.raystack.firehose.config.enums.HttpSinkDataFormatType;
import org.raystack.firehose.config.enums.HttpSinkParameterPlacementType;
import org.raystack.firehose.config.enums.HttpSinkParameterSourceType;
import org.raystack.firehose.config.enums.HttpSinkRequestMethodType;
import org.raystack.firehose.message.Message;
import org.raystack.firehose.proto.ProtoToFieldMapper;
import org.raystack.firehose.sink.http.request.body.JsonBody;
import org.raystack.firehose.sink.http.request.entity.RequestEntityBuilder;
import org.raystack.firehose.sink.http.request.header.HeaderBuilder;
import org.raystack.firehose.sink.http.request.uri.UriBuilder;
import org.raystack.depot.metrics.StatsDReporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.gradle.internal.impldep.org.junit.Assert.assertFalse;
import static org.gradle.internal.impldep.org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class ParameterizedUriRequestTest {
    @Mock
    private UriBuilder uriBuilder;

    @Mock
    private HeaderBuilder headerBuilder;

    @Mock
    private RequestEntityBuilder requestEntityBuilder;

    @Mock
    private JsonBody jsonBody;

    @Mock
    private HttpSinkConfig httpSinkConfig;

    @Mock
    private Message message;

    @Mock
    private ProtoToFieldMapper protoToFieldMapper;

    @Mock
    private StatsDReporter statsDReporter;

    private ParameterizedUriRequest parameterizedURIRequest;
    private HttpSinkRequestMethodType httpSinkRequestMethodType;

    @Before
    public void setup() {
        initMocks(this);
        httpSinkRequestMethodType = HttpSinkRequestMethodType.POST;
        when(httpSinkConfig.getSinkHttpServiceUrl()).thenReturn("http://127.0.0.1:1080/api");
    }

    @Test
    public void shouldProcessForParametrizedQuery() {
        when(httpSinkConfig.getSinkHttpParameterSource()).thenReturn(HttpSinkParameterSourceType.MESSAGE);
        when(httpSinkConfig.getSinkHttpParameterPlacement()).thenReturn(HttpSinkParameterPlacementType.QUERY);

        parameterizedURIRequest = new ParameterizedUriRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType, protoToFieldMapper);
        boolean canProcess = parameterizedURIRequest.canProcess();
        assertTrue(canProcess);
    }

    @Test
    public void shouldNotProcessIfParameterPlacementDisabled() {
        when(httpSinkConfig.getSinkHttpParameterSource()).thenReturn(HttpSinkParameterSourceType.DISABLED);

        parameterizedURIRequest = new ParameterizedUriRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType, protoToFieldMapper);
        boolean canProcess = parameterizedURIRequest.canProcess();

        assertFalse(canProcess);
    }

    @Test
    public void shouldNotProcessIfParameterPlacedInHeader() {
        when(httpSinkConfig.getSinkHttpParameterSource()).thenReturn(HttpSinkParameterSourceType.MESSAGE);
        when(httpSinkConfig.getSinkHttpParameterPlacement()).thenReturn(HttpSinkParameterPlacementType.HEADER);

        parameterizedURIRequest = new ParameterizedUriRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType, protoToFieldMapper);
        boolean canProcess = parameterizedURIRequest.canProcess();

        assertFalse(canProcess);
    }

    @Test
    public void shouldNotProcessTemplatesIfAbsent() {
        parameterizedURIRequest = new ParameterizedUriRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType, protoToFieldMapper);
        boolean isTemplate = parameterizedURIRequest.isTemplateBody(httpSinkConfig);

        assertFalse(isTemplate);
    }

    @Test
    public void shouldProcessTemplatesIfPresent() {
        when(httpSinkConfig.getSinkHttpDataFormat()).thenReturn(HttpSinkDataFormatType.JSON);
        when(httpSinkConfig.getSinkHttpJsonBodyTemplate()).thenReturn("{\"test\":\"$.routes[0]\", \"$.order_number\" : \"xxx\"}");

        parameterizedURIRequest = new ParameterizedUriRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType, protoToFieldMapper);
        boolean isTemplate = parameterizedURIRequest.isTemplateBody(httpSinkConfig);

        assertTrue(isTemplate);
    }

    @org.junit.Test
    public void shouldCheckForTemplateWhileBuilding() throws URISyntaxException {
        when(httpSinkConfig.getSinkHttpDataFormat()).thenReturn(HttpSinkDataFormatType.JSON);
        when(httpSinkConfig.getSinkHttpParameterSource()).thenReturn(HttpSinkParameterSourceType.MESSAGE);
        when(httpSinkConfig.getSinkHttpDataFormat()).thenReturn(HttpSinkDataFormatType.JSON);
        when(httpSinkConfig.getSinkHttpJsonBodyTemplate()).thenReturn("{\"test\":\"$.routes[0]\", \"$.order_number\" : \"xxx\"}");
        when(jsonBody.serialize(any())).thenReturn(Collections.singletonList("test"));
        when(requestEntityBuilder.setWrapping(false)).thenReturn(requestEntityBuilder);
        when(uriBuilder.withParameterizedURI(protoToFieldMapper, HttpSinkParameterSourceType.MESSAGE)).thenReturn(uriBuilder);

        parameterizedURIRequest = new ParameterizedUriRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType, protoToFieldMapper);
        Request request = parameterizedURIRequest.setRequestStrategy(headerBuilder, uriBuilder, requestEntityBuilder);
        request.build(Collections.singletonList(message));

        verify(httpSinkConfig, times(1)).getSinkHttpDataFormat();
        verify(httpSinkConfig, times(1)).getSinkHttpJsonBodyTemplate();
    }

    @org.junit.Test
    public void shouldProcessMessagesIndividually() throws URISyntaxException {
        List<String> serializedMessages = Arrays.asList("Hello", "World!", "How");
        List<Message> messages = Arrays.asList(message, message, message);
        when(httpSinkConfig.getSinkHttpDataFormat()).thenReturn(HttpSinkDataFormatType.JSON);
        when(httpSinkConfig.getSinkHttpParameterSource()).thenReturn(HttpSinkParameterSourceType.MESSAGE);
        when(httpSinkConfig.getSinkHttpJsonBodyTemplate()).thenReturn("{\"test\":\"$.routes[0]\", \"$.order_number\" : \"xxx\"}");
        when(jsonBody.serialize(any())).thenReturn(serializedMessages);
        when(requestEntityBuilder.setWrapping(false)).thenReturn(requestEntityBuilder);
        when(uriBuilder.withParameterizedURI(protoToFieldMapper, HttpSinkParameterSourceType.MESSAGE)).thenReturn(uriBuilder);

        parameterizedURIRequest = new ParameterizedUriRequest(statsDReporter, httpSinkConfig, jsonBody, httpSinkRequestMethodType, protoToFieldMapper);
        Request request = parameterizedURIRequest
                .setRequestStrategy(headerBuilder, uriBuilder, requestEntityBuilder);
        request.build(messages);

        verify(uriBuilder, times(3)).build(message);
        verify(headerBuilder, times(3)).build(message);
        verify(requestEntityBuilder, times(3)).buildHttpEntity(any(String.class));
    }
}
