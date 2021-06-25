package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sinkdecorator.dlq.DlqWriter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class SinkWithDlqTest {

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

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, instrumentation);

        sinkWithDlq.pushMessage(messages);
    }

    @Test
    public void shouldReturnUnsuccessfulWrite() throws IOException {
        when(dlqWriter.write(anyList())).thenReturn(Arrays.asList(message));
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(messages);

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, instrumentation);

        List<Message> unsuccessfulWrite = sinkWithDlq.pushMessage(messages);
        verify(instrumentation).logError("failed to write {} number messages to DLQ", 1);
        assertEquals(1, unsuccessfulWrite.size());
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenWriterThrowIOException() throws IOException {
        when(dlqWriter.write(anyList())).thenThrow(new IOException());
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(messages);

        SinkWithDlq sinkWithDlq = new SinkWithDlq(sinkWithRetry, dlqWriter, instrumentation);

        sinkWithDlq.pushMessage(messages);
    }
}
