package io.odpf.firehose.consumer;

import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.tracer.SinkTracer;
import io.odpf.firehose.util.Clock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class FirehoseAsyncConsumerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private ExecutorService executorService;
    @Mock
    private Sink sink;
    @Mock
    private Clock clock;
    @Mock
    private SinkTracer tracer;
    @Mock
    private Instrumentation instrumentation;
    @Mock
    private Future<List<Message>> future1;
    @Mock
    private Future<List<Message>> future2;
    @Mock
    private ConsumerAndOffsetManager consumerAndOffsetManager;
    private FirehoseAsyncConsumer asyncConsumer;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        this.asyncConsumer = new FirehoseAsyncConsumer(sink, clock, tracer, consumerAndOffsetManager, instrumentation, executorService);
    }

    @Test
    public void shouldPushMessagesInParallel() throws FilterException, IOException {
        List<Message> messageList1 = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
            add(new Message(new byte[0], new byte[0], "topic1", 2, 11));
            add(new Message(new byte[0], new byte[0], "topic1", 3, 12));
        }};

        List<Message> messageList2 = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 2, 5));
            add(new Message(new byte[0], new byte[0], "topic1", 2, 6));
        }};

        Mockito.when(consumerAndOffsetManager.readMessagesFromKafka()).thenReturn(messageList1);
        Mockito.when(sink.pushMessage(messageList1)).thenReturn(new ArrayList<>());
        Mockito.when(executorService.submit(new FirehoseAsyncConsumer.SinkTask(sink, messageList1))).thenReturn(future1);
        Mockito.when(executorService.submit(new FirehoseAsyncConsumer.SinkTask(sink, messageList2))).thenReturn(future2);

        Mockito.when(future1.isDone()).thenReturn(false);
        Mockito.when(future2.isDone()).thenReturn(false);
        asyncConsumer.process();

        Mockito.when(consumerAndOffsetManager.readMessagesFromKafka()).thenReturn(messageList2);
        Mockito.when(sink.pushMessage(messageList2)).thenReturn(new ArrayList<>());
        asyncConsumer.process();
        Mockito.verify(consumerAndOffsetManager, Mockito.times(1)).addOffsets(future1, messageList1);
        Mockito.verify(consumerAndOffsetManager, Mockito.times(1)).addOffsets(future2, messageList2);
        Mockito.verify(consumerAndOffsetManager, Mockito.times(0)).setCommittable(Mockito.any());
    }

    @Test
    public void shouldCallSetCommittableForDoneFutures() throws FilterException, IOException {
        List<Message> messages = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 11));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 12));
        }};
        Mockito.when(consumerAndOffsetManager.readMessagesFromKafka()).thenReturn(messages);

        Mockito.when(sink.pushMessage(messages)).thenReturn(new ArrayList<>());

        Mockito.when(executorService.submit(new FirehoseAsyncConsumer.SinkTask(sink, messages))).thenReturn(future1);
        Mockito.when(future1.isDone()).thenReturn(true);
        asyncConsumer.process();

        Mockito.verify(consumerAndOffsetManager, Mockito.times(1)).addOffsets(future1, messages);
        Mockito.verify(consumerAndOffsetManager, Mockito.times(1)).setCommittable(future1);
    }

    @Test
    public void shouldThrowExceptionIfSinkTaskFails() throws Exception {
        expectedException.expect(AsyncConsumerFailedException.class);
        List<Message> messages = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 11));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 12));
        }};
        Mockito.when(consumerAndOffsetManager.readMessagesFromKafka()).thenReturn(messages);

        Mockito.when(sink.pushMessage(messages)).thenReturn(new ArrayList<>());

        Mockito.when(executorService.submit(new FirehoseAsyncConsumer.SinkTask(sink, messages))).thenReturn(future1);
        Mockito.when(future1.isDone()).thenReturn(true);
        Mockito.when(future1.get()).thenThrow(new ExecutionException(new RuntimeException()));
        asyncConsumer.process();
    }
}
