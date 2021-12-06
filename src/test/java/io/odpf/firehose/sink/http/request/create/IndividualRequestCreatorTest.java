package io.odpf.firehose.sink.http.request.create;

import io.odpf.firehose.config.enums.HttpSinkRequestMethodType;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.http.request.body.JsonBody;
import io.odpf.firehose.sink.http.request.entity.RequestEntityBuilder;
import io.odpf.firehose.sink.http.request.header.HeaderBuilder;
import io.odpf.firehose.sink.http.request.uri.UriBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class IndividualRequestCreatorTest {
    @Mock
    private UriBuilder uriBuilder;

    @Mock
    private HeaderBuilder headerBuilder;

    @Mock
    private RequestEntityBuilder requestEntityBuilder;

    @Mock
    private JsonBody jsonBody;

    @Mock
    private Instrumentation instrumentation;

    @Before
    public void setup() {
        initMocks(this);
        Message message = new Message(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
    }

    @Test
    public void shouldProduceIndividualRequests() throws DeserializerException, URISyntaxException {
        Message message1 = new Message(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        Message message2 = new Message(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message1);
        messages.add(message2);

        ArrayList<String> serializedMessages = new ArrayList<>();
        serializedMessages.add("dummyMessage1");
        serializedMessages.add("dummyMessage2");
        when(jsonBody.serialize(messages)).thenReturn(serializedMessages);

        IndividualRequestCreator individualRequestCreator = new IndividualRequestCreator(instrumentation, uriBuilder, headerBuilder, HttpSinkRequestMethodType.PUT, jsonBody);
        List<HttpEntityEnclosingRequestBase> requests = individualRequestCreator.create(messages, requestEntityBuilder);

        assertEquals(2, requests.size());
        verify(instrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(messages).get(0), HttpSinkRequestMethodType.PUT);
        verify(instrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(messages).get(1), HttpSinkRequestMethodType.PUT);
    }

    @Test
    public void shouldSetRequestPropertiesMultipleTimes() throws DeserializerException, URISyntaxException {
        Message message1 = new Message(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        Message message2 = new Message(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message1);
        messages.add(message2);

        ArrayList<String> serializedMessages = new ArrayList<>();
        serializedMessages.add("dummyMessage1");
        serializedMessages.add("dummyMessage2");
        when(jsonBody.serialize(messages)).thenReturn(serializedMessages);

        IndividualRequestCreator individualRequestCreator = new IndividualRequestCreator(instrumentation, uriBuilder, headerBuilder, HttpSinkRequestMethodType.PUT, jsonBody);
        individualRequestCreator.create(messages, requestEntityBuilder);

        verify(uriBuilder, times(2)).build(any(Message.class));
        verify(headerBuilder, times(2)).build(any(Message.class));
        verify(requestEntityBuilder, times(2)).buildHttpEntity(any(String.class));
        verify(instrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(messages).get(0), HttpSinkRequestMethodType.PUT);
        verify(instrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(messages).get(1), HttpSinkRequestMethodType.PUT);
    }

    @Test
    public void shouldProduceIndividualRequestsWhenPUTRequest() throws DeserializerException, URISyntaxException {
        Message message1 = new Message(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        Message message2 = new Message(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message1);
        messages.add(message2);

        ArrayList<String> serializedMessages = new ArrayList<>();
        serializedMessages.add("dummyMessage1");
        serializedMessages.add("dummyMessage2");
        when(jsonBody.serialize(messages)).thenReturn(serializedMessages);

        IndividualRequestCreator individualRequestCreator = new IndividualRequestCreator(instrumentation, uriBuilder, headerBuilder, HttpSinkRequestMethodType.PUT, jsonBody);
        List<HttpEntityEnclosingRequestBase> requests = individualRequestCreator.create(messages, requestEntityBuilder);

        assertEquals(2, requests.size());
        verify(instrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(messages).get(0), HttpSinkRequestMethodType.PUT);
        verify(instrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(messages).get(1), HttpSinkRequestMethodType.PUT);
    }

    @Test
    public void shouldWrapEntityToArrayIfSet() throws DeserializerException, URISyntaxException, IOException {
        Message message1 = new Message(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        Message message2 = new Message(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message1);
        messages.add(message2);

        ArrayList<String> serializedMessages = new ArrayList<>();
        serializedMessages.add("dummyMessage1");
        serializedMessages.add("dummyMessage2");
        when(jsonBody.serialize(messages)).thenReturn(serializedMessages);

        requestEntityBuilder = new RequestEntityBuilder().setWrapping(true);

        IndividualRequestCreator individualRequestCreator = new IndividualRequestCreator(instrumentation, uriBuilder, headerBuilder, HttpSinkRequestMethodType.PUT, jsonBody);
        List<HttpEntityEnclosingRequestBase> requests = individualRequestCreator.create(messages, requestEntityBuilder);

        byte[] bytes1 = IOUtils.toByteArray(requests.get(0).getEntity().getContent());
        byte[] bytes2 = IOUtils.toByteArray(requests.get(1).getEntity().getContent());
        Assert.assertEquals("[dummyMessage1]", new String(bytes1));
        Assert.assertEquals("[dummyMessage2]", new String(bytes2));

        verify(instrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(messages).get(0), HttpSinkRequestMethodType.PUT);
        verify(instrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(messages).get(1), HttpSinkRequestMethodType.PUT);
    }

    @Test
    public void shouldNotWrapEntityToArrayIfNot() throws DeserializerException, URISyntaxException, IOException {
        Message message1 = new Message(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        Message message2 = new Message(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message1);
        messages.add(message2);

        ArrayList<String> serializedMessages = new ArrayList<>();
        serializedMessages.add("dummyMessage1");
        serializedMessages.add("dummyMessage2");
        when(jsonBody.serialize(messages)).thenReturn(serializedMessages);

        requestEntityBuilder = new RequestEntityBuilder().setWrapping(false);

        IndividualRequestCreator individualRequestCreator = new IndividualRequestCreator(instrumentation, uriBuilder, headerBuilder, HttpSinkRequestMethodType.PUT, jsonBody);
        List<HttpEntityEnclosingRequestBase> requests = individualRequestCreator.create(messages, requestEntityBuilder);

        byte[] bytes1 = IOUtils.toByteArray(requests.get(0).getEntity().getContent());
        byte[] bytes2 = IOUtils.toByteArray(requests.get(1).getEntity().getContent());
        Assert.assertEquals("dummyMessage1", new String(bytes1));
        Assert.assertEquals("dummyMessage2", new String(bytes2));

        verify(instrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(messages).get(0), HttpSinkRequestMethodType.PUT);
        verify(instrumentation, times(1)).logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uriBuilder.build(), headerBuilder.build(), jsonBody.serialize(messages).get(1), HttpSinkRequestMethodType.PUT);
    }
}
