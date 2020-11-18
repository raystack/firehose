package com.gojek.esb.sink.http.request.create;

import com.gojek.esb.config.enums.HttpRequestMethod;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.entity.RequestEntityBuilder;
import com.gojek.esb.sink.http.request.header.HeaderBuilder;
import com.gojek.esb.sink.http.request.uri.URIBuilder;
import org.apache.http.Header;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class BatchRequestCreatorTest {

    @Mock
    private URIBuilder uriBuilder;

    @Mock
    private HeaderBuilder headerBuilder;

    @Mock
    private RequestEntityBuilder requestEntityBuilder;

    @Mock
    private JsonBody jsonBody;

    @Mock
    private Instrumentation instrumentation;

    private List<EsbMessage> esbMessages;

    @Before
    public void setup() {
        initMocks(this);
        EsbMessage esbMessage = new EsbMessage(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        esbMessages = Collections.singletonList(esbMessage);
    }

    @Test
    public void shouldWrapMessageToASingleRequest() throws DeserializerException, URISyntaxException {
        BatchRequestCreator batchRequestCreator = new BatchRequestCreator(instrumentation, uriBuilder, headerBuilder, HttpRequestMethod.PUT, jsonBody);
        List<HttpEntityEnclosingRequestBase> requests = batchRequestCreator.create(esbMessages, requestEntityBuilder);

        assertEquals(1, requests.size());
        verify(instrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(esbMessages), HttpRequestMethod.PUT);
    }

    @Test
    public void shouldWrapMessageToASingleRequestWhenPostRequest() throws DeserializerException, URISyntaxException {
        BatchRequestCreator batchRequestCreator = new BatchRequestCreator(instrumentation, uriBuilder, headerBuilder, HttpRequestMethod.POST, jsonBody);
        List<HttpEntityEnclosingRequestBase> requests = batchRequestCreator.create(esbMessages, requestEntityBuilder);

        assertEquals(1, requests.size());
        verify(instrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(esbMessages), HttpRequestMethod.POST);
    }

    @Test
    public void shouldWrapMessagesToASingleRequest() throws DeserializerException, URISyntaxException {
        EsbMessage esbMessage1 = new EsbMessage(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        EsbMessage esbMessage2 = new EsbMessage(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        esbMessages = new ArrayList<>();
        esbMessages.add(esbMessage1);
        esbMessages.add(esbMessage2);

        BatchRequestCreator batchRequestCreator = new BatchRequestCreator(instrumentation, uriBuilder, headerBuilder, HttpRequestMethod.PUT, jsonBody);
        List<HttpEntityEnclosingRequestBase> requests = batchRequestCreator.create(esbMessages, requestEntityBuilder);

        assertEquals(1, requests.size());
        verify(instrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(esbMessages), HttpRequestMethod.PUT);
    }

    @Test
    public void shouldSetRequestPropertiesOnlyOnce() throws DeserializerException, URISyntaxException {
        EsbMessage esbMessage1 = new EsbMessage(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        EsbMessage esbMessage2 = new EsbMessage(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        esbMessages = new ArrayList<>();
        esbMessages.add(esbMessage1);
        esbMessages.add(esbMessage2);

        BatchRequestCreator batchRequestCreator = new BatchRequestCreator(instrumentation, uriBuilder, headerBuilder, HttpRequestMethod.POST, jsonBody);
        batchRequestCreator.create(esbMessages, requestEntityBuilder);

        verify(uriBuilder, times(1)).build();
        verify(headerBuilder, times(1)).build();
        verify(requestEntityBuilder, times(1)).buildHttpEntity(any(String.class));
        verify(instrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(esbMessages), HttpRequestMethod.POST);
    }

    @Test
    public void shouldProperlyBuildRequests() throws DeserializerException, URISyntaxException {
        EsbMessage esbMessage1 = new EsbMessage(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        EsbMessage esbMessage2 = new EsbMessage(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        esbMessages = new ArrayList<>();
        esbMessages.add(esbMessage1);
        esbMessages.add(esbMessage2);

        HashMap<String, String> headerMap = new HashMap<>();
        headerMap.put("Authorization", "auth_token");
        headerMap.put("Accept", "text/plain");

        ArrayList<String> serializedMessages = new ArrayList<>();
        serializedMessages.add("dummyMessage1");
        serializedMessages.add("dummyMessage2");

        when(uriBuilder.build()).thenReturn(new URI("dummyEndpoint"));
        when(headerBuilder.build()).thenReturn(headerMap);
        when(jsonBody.serialize(esbMessages)).thenReturn(serializedMessages);
        when(requestEntityBuilder.buildHttpEntity(any())).thenReturn(new StringEntity("[\"dummyMessage1\", \"dummyMessage2\"]", ContentType.APPLICATION_JSON));

        BatchRequestCreator batchRequestCreator = new BatchRequestCreator(instrumentation, uriBuilder, headerBuilder, HttpRequestMethod.POST, jsonBody);
        List<HttpEntityEnclosingRequestBase> httpEntityEnclosingRequestBases = batchRequestCreator.create(esbMessages, requestEntityBuilder);

        BasicHeader header1 = new BasicHeader("Authorization", "auth_token");
        BasicHeader header2 = new BasicHeader("Accept", "text/plain");
        Header[] headers = new Header[2];
        headers[0] = header1;
        headers[1] = header2;

        assertEquals(new URI("dummyEndpoint"), httpEntityEnclosingRequestBases.get(0).getURI());
        Assert.assertTrue(new ReflectionEquals(httpEntityEnclosingRequestBases.get(0).getAllHeaders()).matches(headers));
        verify(instrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(esbMessages), HttpRequestMethod.POST);
    }
}
