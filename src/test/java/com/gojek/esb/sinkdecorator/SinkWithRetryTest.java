package com.gojek.esb.sinkdecorator;

import com.gojek.esb.consumer.EsbMessage;
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
    private EsbMessage esbMessage;

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
        List<EsbMessage> esbMessages = sinkWithRetry.pushMessage(
                Collections.singletonList(new EsbMessage("key".getBytes(), "value".getBytes(), "topic", 1, 1)));

        assertTrue(esbMessages.isEmpty());
        verify(sinkDecorator, Mockito.times(1)).pushMessage(anyList());
    }

    @Test
    public void shouldRetryForNumberOfAttemptsIfSuperReturnsEsbMessages() throws IOException, DeserializerException {
        ArrayList<EsbMessage> messages = new ArrayList<>();
        messages.add(esbMessage);
        messages.add(esbMessage);
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(messages).thenReturn(messages)
                .thenReturn(messages);
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, instrumentation, 3, parser);

        List<EsbMessage> esbMessages = sinkWithRetry.pushMessage(Collections.singletonList(esbMessage));

        assertFalse(esbMessages.isEmpty());
        verify(sinkDecorator, Mockito.times(4)).pushMessage(anyList());
    }

    @Test
    public void shouldRetryForNumberOfAttemptsAndSendEmptyMessageOnSuccess() throws IOException, DeserializerException {
        ArrayList<EsbMessage> messages = new ArrayList<>();
        messages.add(esbMessage);
        messages.add(esbMessage);
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(messages)
                .thenReturn(new ArrayList<>());
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, instrumentation, 3, parser);

        List<EsbMessage> esbMessages = sinkWithRetry.pushMessage(Collections.singletonList(esbMessage));

        assertTrue(esbMessages.isEmpty());
        verify(sinkDecorator, Mockito.times(3)).pushMessage(anyList());
    }

    @Test
    public void shouldRetryUntilSuccess() throws IOException, DeserializerException {
        ArrayList<EsbMessage> messages = new ArrayList<>();
        messages.add(esbMessage);
        messages.add(esbMessage);
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(messages).thenReturn(messages)
                .thenReturn(messages).thenReturn(messages).thenReturn(new ArrayList<>());
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, instrumentation, parser);

        List<EsbMessage> esbMessages = sinkWithRetry.pushMessage(Collections.singletonList(esbMessage));

        assertTrue(esbMessages.isEmpty());
        verify(sinkDecorator, Mockito.times(6)).pushMessage(anyList());
    }

    @Test
    public void shouldLogRetriesMessages() throws IOException, DeserializerException {
        ArrayList<EsbMessage> messages = new ArrayList<>();
        int maxRetryAttempts = 10;
        messages.add(esbMessage);
        messages.add(esbMessage);
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(messages).thenReturn(messages)
                .thenReturn(messages).thenReturn(messages).thenReturn(new ArrayList<>());
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, instrumentation, maxRetryAttempts, parser);

        List<EsbMessage> esbMessages = sinkWithRetry.pushMessage(Collections.singletonList(esbMessage));
        assertTrue(esbMessages.isEmpty());
        verify(instrumentation, times(1)).logWarn("Maximum retry attemps: {}", 10);
        verify(instrumentation, times(5)).incrementCounter("request_retries");
        verify(instrumentation, times(1)).logWarn("Retrying messages attempt count: {}, Number of messages: {}", 1, 2);
        verify(instrumentation, times(1)).logWarn("Retrying messages attempt count: {}, Number of messages: {}", 2, 2);
        verify(instrumentation, times(1)).logWarn("Retrying messages attempt count: {}, Number of messages: {}", 3, 2);
        verify(instrumentation, times(1)).logWarn("Retrying messages attempt count: {}, Number of messages: {}", 4, 2);
        verify(instrumentation, times(1)).logWarn("Retrying messages attempt count: {}, Number of messages: {}", 5, 2);
        verify(instrumentation, times(5)).logDebug("Retry failed messages: \n{}", "[null, null]");
    }
}
