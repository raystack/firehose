package io.odpf.firehose.sink.http.request.create;

import io.odpf.firehose.config.enums.HttpSinkRequestMethodType;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.firehose.sink.http.request.body.JsonBody;
import io.odpf.firehose.sink.http.request.entity.RequestEntityBuilder;
import io.odpf.firehose.sink.http.request.header.HeaderBuilder;
import io.odpf.firehose.sink.http.request.uri.UriBuilder;
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
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class BatchRequestCreatorTest {

    @Mock
    private UriBuilder uriBuilder;

    @Mock
    private HeaderBuilder headerBuilder;

    @Mock
    private RequestEntityBuilder requestEntityBuilder;

    @Mock
    private JsonBody jsonBody;

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    private List<Message> messages;

    @Before
    public void setup() {
        initMocks(this);
        Message message = new Message(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        messages = Collections.singletonList(message);
    }

    @Test
    public void shouldWrapMessageToASingleRequest() throws DeserializerException, URISyntaxException {
        BatchRequestCreator batchRequestCreator = new BatchRequestCreator(firehoseInstrumentation, uriBuilder, headerBuilder, HttpSinkRequestMethodType.PUT, jsonBody);
        List<HttpEntityEnclosingRequestBase> requests = batchRequestCreator.create(messages, requestEntityBuilder);

        assertEquals(1, requests.size());
        assertEquals(HttpSinkRequestMethodType.PUT.toString(), requests.get(0).getMethod());
        verify(firehoseInstrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(messages), HttpSinkRequestMethodType.PUT);
    }

    @Test
    public void shouldWrapMessageToASingleRequestWhenPostRequest() throws DeserializerException, URISyntaxException {
        BatchRequestCreator batchRequestCreator = new BatchRequestCreator(firehoseInstrumentation, uriBuilder, headerBuilder, HttpSinkRequestMethodType.POST, jsonBody);
        List<HttpEntityEnclosingRequestBase> requests = batchRequestCreator.create(messages, requestEntityBuilder);

        assertEquals(1, requests.size());
        assertEquals(HttpSinkRequestMethodType.POST.toString(), requests.get(0).getMethod());
        verify(firehoseInstrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(messages), HttpSinkRequestMethodType.POST);
    }

    @Test
    public void shouldWrapMessageToASingleRequestWhenPatchRequest() throws DeserializerException, URISyntaxException {
        BatchRequestCreator batchRequestCreator = new BatchRequestCreator(firehoseInstrumentation, uriBuilder, headerBuilder, HttpSinkRequestMethodType.PATCH, jsonBody);
        List<HttpEntityEnclosingRequestBase> requests = batchRequestCreator.create(messages, requestEntityBuilder);

        assertEquals(1, requests.size());
        assertEquals(HttpSinkRequestMethodType.PATCH.toString(), requests.get(0).getMethod());
        verify(firehoseInstrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(messages), HttpSinkRequestMethodType.PATCH);
    }

    @Test
    public void shouldWrapMessageToASingleRequestWhenDeleteRequest() throws DeserializerException, URISyntaxException {
        BatchRequestCreator batchRequestCreator = new BatchRequestCreator(firehoseInstrumentation, uriBuilder, headerBuilder, HttpSinkRequestMethodType.DELETE, jsonBody);
        List<HttpEntityEnclosingRequestBase> requests = batchRequestCreator.create(messages, requestEntityBuilder);

        assertEquals(1, requests.size());
        assertEquals(HttpSinkRequestMethodType.DELETE.toString(), requests.get(0).getMethod());
        verify(firehoseInstrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(messages), HttpSinkRequestMethodType.DELETE);
    }

    @Test
    public void shouldWrapMessagesToASingleRequest() throws DeserializerException, URISyntaxException {
        Message message1 = new Message(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        Message message2 = new Message(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        messages = new ArrayList<>();
        messages.add(message1);
        messages.add(message2);

        BatchRequestCreator batchRequestCreator = new BatchRequestCreator(firehoseInstrumentation, uriBuilder, headerBuilder, HttpSinkRequestMethodType.PUT, jsonBody);
        List<HttpEntityEnclosingRequestBase> requests = batchRequestCreator.create(messages, requestEntityBuilder);

        assertEquals(1, requests.size());
        verify(firehoseInstrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(messages), HttpSinkRequestMethodType.PUT);
    }

    @Test
    public void shouldSetRequestPropertiesOnlyOnce() throws DeserializerException, URISyntaxException {
        Message message1 = new Message(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        Message message2 = new Message(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        messages = new ArrayList<>();
        messages.add(message1);
        messages.add(message2);

        BatchRequestCreator batchRequestCreator = new BatchRequestCreator(firehoseInstrumentation, uriBuilder, headerBuilder, HttpSinkRequestMethodType.POST, jsonBody);
        batchRequestCreator.create(messages, requestEntityBuilder);

        verify(uriBuilder, times(1)).build();
        verify(headerBuilder, times(1)).build();
        verify(requestEntityBuilder, times(1)).buildHttpEntity(any(String.class));
        verify(firehoseInstrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(messages), HttpSinkRequestMethodType.POST);
    }

    @Test
    public void shouldProperlyBuildRequests() throws DeserializerException, URISyntaxException {
        Message message1 = new Message(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        Message message2 = new Message(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        messages = new ArrayList<>();
        messages.add(message1);
        messages.add(message2);

        HashMap<String, String> headerMap = new HashMap<>();
        headerMap.put("Authorization", "auth_token");
        headerMap.put("Accept", "text/plain");

        ArrayList<String> serializedMessages = new ArrayList<>();
        serializedMessages.add("dummyMessage1");
        serializedMessages.add("dummyMessage2");

        when(uriBuilder.build()).thenReturn(new URI("dummyEndpoint"));
        when(headerBuilder.build()).thenReturn(headerMap);
        when(jsonBody.serialize(messages)).thenReturn(serializedMessages);
        when(requestEntityBuilder.buildHttpEntity(any())).thenReturn(new StringEntity("[\"dummyMessage1\", \"dummyMessage2\"]", ContentType.APPLICATION_JSON));

        BatchRequestCreator batchRequestCreator = new BatchRequestCreator(firehoseInstrumentation, uriBuilder, headerBuilder, HttpSinkRequestMethodType.POST, jsonBody);
        List<HttpEntityEnclosingRequestBase> httpEntityEnclosingRequestBases = batchRequestCreator.create(messages, requestEntityBuilder);

        BasicHeader header1 = new BasicHeader("Authorization", "auth_token");
        BasicHeader header2 = new BasicHeader("Accept", "text/plain");
        Header[] headers = new Header[2];
        headers[0] = header1;
        headers[1] = header2;

        assertEquals(new URI("dummyEndpoint"), httpEntityEnclosingRequestBases.get(0).getURI());
        Assert.assertTrue(new ReflectionEquals(httpEntityEnclosingRequestBases.get(0).getAllHeaders()).matches(headers));
        verify(firehoseInstrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(messages), HttpSinkRequestMethodType.POST);
    }
}
