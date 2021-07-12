package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.config.DlqConfig;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.log.KeyOrMessageParser;
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

    @Mock
    private ErrorMatcher errorMatcher;

    @Mock
    private DlqConfig dlqConfig;

    @Before
    public void setUp() {
        initMocks(this);
        when(dlqConfig.getDlqAttemptsToTrigger()).thenReturn(3);
        when(dlqConfig.getFailOnMaxRetryAttempts()).thenReturn(false);
    }

    @Test
    public void shouldReturnEmptyListIfSuperReturnsEmptyList() throws IOException, DeserializerException {
        when(sinkDecorator.pushMessage(anyList())).thenReturn(new ArrayList<>());
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, instrumentation, dlqConfig, parser, errorMatcher);
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
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, instrumentation, dlqConfig, parser, errorMatcher);

        List<Message> messageList = sinkWithRetry.pushMessage(Collections.singletonList(message));

        assertFalse(messageList.isEmpty());
        verify(sinkDecorator, Mockito.times(4)).pushMessage(anyList());
    }

    @Test
    public void shouldRetryForNumberOfAttemptsAndSendEmptyMessageOnSuccess() throws IOException, DeserializerException {
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(messages)
                .thenReturn(new ArrayList<>());
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, instrumentation, dlqConfig, parser, errorMatcher);

        List<Message> messageList = sinkWithRetry.pushMessage(Collections.singletonList(message));

        assertTrue(messageList.isEmpty());
        verify(sinkDecorator, Mockito.times(3)).pushMessage(anyList());
    }

    @Test
    public void shouldRetryUntilSuccess() throws IOException, DeserializerException {
        when(dlqConfig.getDlqAttemptsToTrigger()).thenReturn(Integer.MAX_VALUE);

        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(messages).thenReturn(messages)
                .thenReturn(messages).thenReturn(messages).thenReturn(new ArrayList<>());
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, instrumentation, dlqConfig, parser, errorMatcher);

        List<Message> messageList = sinkWithRetry.pushMessage(Collections.singletonList(message));

        assertTrue(messageList.isEmpty());
        verify(sinkDecorator, Mockito.times(6)).pushMessage(anyList());
    }

    @Test
    public void shouldLogRetriesMessages() throws IOException, DeserializerException {
        when(dlqConfig.getDlqAttemptsToTrigger()).thenReturn(10);
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(messages).thenReturn(messages)
                .thenReturn(messages).thenReturn(messages).thenReturn(new ArrayList<>());
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, instrumentation, dlqConfig, parser, errorMatcher);

        List<Message> messageList = sinkWithRetry.pushMessage(Collections.singletonList(message));
        assertTrue(messageList.isEmpty());
        verify(instrumentation, times(1)).logWarn("Maximum retry attemps: {}", 10);
        verify(instrumentation, times(5)).incrementCounter("firehose_retry_total");
        verify(instrumentation, times(1)).logWarn("Retrying messages attempt count: {}, Number of messages: {}", 1, 2);
        verify(instrumentation, times(1)).logWarn("Retrying messages attempt count: {}, Number of messages: {}", 2, 2);
        verify(instrumentation, times(1)).logWarn("Retrying messages attempt count: {}, Number of messages: {}", 3, 2);
        verify(instrumentation, times(1)).logWarn("Retrying messages attempt count: {}, Number of messages: {}", 4, 2);
        verify(instrumentation, times(1)).logWarn("Retrying messages attempt count: {}, Number of messages: {}", 5, 2);
        verify(instrumentation, times(5)).logDebug("Retry failed messages: \n{}", "[null, null]");
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenExceedMaximumRetryAttempts() throws IOException {
        when(dlqConfig.getDlqAttemptsToTrigger()).thenReturn(4);
        when(dlqConfig.getFailOnMaxRetryAttempts()).thenReturn(true);

        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(sinkDecorator.pushMessage(anyList())).thenReturn(messages).thenReturn(messages).thenReturn(messages)
                .thenReturn(messages).thenReturn(messages).thenReturn(new ArrayList<>());
        SinkWithRetry sinkWithRetry = new SinkWithRetry(sinkDecorator, backOffProvider, instrumentation, dlqConfig, parser, errorMatcher);

        sinkWithRetry.pushMessage(Collections.singletonList(message));
    }
}
