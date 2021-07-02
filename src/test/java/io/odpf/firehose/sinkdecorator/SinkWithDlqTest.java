package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.consumer.ErrorInfo;
import io.odpf.firehose.consumer.ErrorType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sinkdecorator.dlq.DlqWriter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class SinkWithDlqTest {

    @Mock
    private BackOffProvider backOffProvider;

    @Mock
    private SinkWithRetry sinkWithRetry;

    @Mock
    private Message message;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private DlqWriter dlqWriter;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldWriteToDLQWriter() throws Exception {
        when(dlqWriter.write(anyList())).thenReturn(new LinkedList<>());
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(messages);

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, instrumentation);

        List<Message> pushResult = sinkWithDlq.pushMessage(messages);
        verify(dlqWriter, times(1)).write(messages);
        assertEquals(0, pushResult.size());
    }


    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenWriterThrowIOException() throws IOException {
        when(dlqWriter.write(anyList())).thenThrow(new IOException());
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(messages);

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, instrumentation);

        sinkWithDlq.pushMessage(messages);
    }

    @Test
    public void shouldRetryWriteMessagesToDlq() throws IOException {
        Message messageWithError = new Message(message, new ErrorInfo(new IOException(), ErrorType.UNKNOWN_ERROR));
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(messageWithError);
        messages.add(messageWithError);

        List<Message> retryMessages = new LinkedList<>();
        retryMessages.add(messageWithError);

        when(sinkWithRetry.pushMessage(messages)).thenReturn(messages);
        when(dlqWriter.write(messages)).thenReturn(retryMessages);
        when(dlqWriter.write(retryMessages)).thenReturn(new ArrayList<>());

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, instrumentation);

        sinkWithDlq.pushMessage(messages);

        verify(dlqWriter, times(1)).write(messages);
        verify(dlqWriter, times(1)).write(retryMessages);
        verify(instrumentation, times(2)).captureRetryAttempts();
        verify(instrumentation, times(2)).incrementMessageSucceedCount();
        verify(instrumentation, times(1)).incrementMessageFailCount(any(), any());
    }

    @Test
    public void shouldCallSinkToManageCommitOffset() throws IOException {
        when(dlqWriter.write(anyList())).thenReturn(new LinkedList<>());
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(messages);

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, backOffProvider, instrumentation);

        List<Message> pushResult = sinkWithDlq.pushMessage(messages);
        verify(sinkWithRetry, times(1)).addOffsetToBatch(SinkWithDlq.DLQ_BATCH_KEY, messages);
        verify(sinkWithRetry, times(1)).setCommittable(SinkWithDlq.DLQ_BATCH_KEY);
        assertEquals(0, pushResult.size());
    }
}
