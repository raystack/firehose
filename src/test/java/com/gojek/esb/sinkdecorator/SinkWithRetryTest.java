package com.gojek.esb.sinkdecorator;

import com.gojek.esb.consumer.Message;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.Instrumentation;

import com.gojek.esb.sink.log.KeyOrMessageParser;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class SinkWithRetryTest {

    @Mock
    private SinkDecorator sinkDecorator;

    @Mock
    private BackOffProvider backOffProvider;

    @Mock
    private Message message;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private KeyOrMessageParser parser;

    @Before
    public void setUp() {
        initMocks(this);
    }

    @Test
    public void shouldReturnEmptyListIfSuperReturnsEmptyList() throws IOException, DeserializerException {
        when(sinkDecorator.pushMessage(anyList())).thenReturn(new ArrayList<>());
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, instrumentation, 3, parser);
        List<Message> messages = sinkWithRetry.pushMessage(
                Collections.singletonList(new Message("key".getBytes(), "value".getBytes(), "topic", 1, 1)));

        assertTrue(messages.isEmpty());
        verify(sinkDecorator, Mockito.times(1)).pushMessage(anyList());
    }

    @Test
    public void shouldRetryForNumberOfAttemptsIfSuperReturnsEsbMessages() throws IOException, DeserializerException {
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(messages).thenReturn(messages)
                .thenReturn(messages);
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, instrumentation, 3, parser);

        List<Message> esbMessages = sinkWithRetry.pushMessage(Collections.singletonList(message));

        assertFalse(esbMessages.isEmpty());
        verify(sinkDecorator, Mockito.times(4)).pushMessage(anyList());
    }

    @Test
    public void shouldRetryForNumberOfAttemptsAndSendEmptyMessageOnSuccess() throws IOException, DeserializerException {
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(messages)
                .thenReturn(new ArrayList<>());
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, instrumentation, 3, parser);

        List<Message> esbMessages = sinkWithRetry.pushMessage(Collections.singletonList(message));

        assertTrue(esbMessages.isEmpty());
        verify(sinkDecorator, Mockito.times(3)).pushMessage(anyList());
    }

    @Test
    public void shouldRetryUntilSuccess() throws IOException, DeserializerException {
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(messages).thenReturn(messages)
                .thenReturn(messages).thenReturn(messages).thenReturn(new ArrayList<>());
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, instrumentation, parser);

        List<Message> esbMessages = sinkWithRetry.pushMessage(Collections.singletonList(message));

        assertTrue(esbMessages.isEmpty());
        verify(sinkDecorator, Mockito.times(6)).pushMessage(anyList());
    }

    @Test
    public void shouldLogRetriesMessages() throws IOException, DeserializerException {
        ArrayList<Message> messages = new ArrayList<>();
        int maxRetryAttempts = 10;
        messages.add(message);
        messages.add(message);
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(messages).thenReturn(messages)
                .thenReturn(messages).thenReturn(messages).thenReturn(new ArrayList<>());
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, instrumentation, maxRetryAttempts, parser);

        List<Message> esbMessages = sinkWithRetry.pushMessage(Collections.singletonList(message));
        assertTrue(esbMessages.isEmpty());
        verify(instrumentation, times(1)).logWarn("Maximum retry attemps: {}", 10);
        verify(instrumentation, times(5)).incrementCounter("firehose_retry_total");
        verify(instrumentation, times(1)).logWarn("Retrying messages attempt count: {}, Number of messages: {}", 1, 2);
        verify(instrumentation, times(1)).logWarn("Retrying messages attempt count: {}, Number of messages: {}", 2, 2);
        verify(instrumentation, times(1)).logWarn("Retrying messages attempt count: {}, Number of messages: {}", 3, 2);
        verify(instrumentation, times(1)).logWarn("Retrying messages attempt count: {}, Number of messages: {}", 4, 2);
        verify(instrumentation, times(1)).logWarn("Retrying messages attempt count: {}, Number of messages: {}", 5, 2);
        verify(instrumentation, times(5)).logDebug("Retry failed messages: \n{}", "[null, null]");
    }
}
