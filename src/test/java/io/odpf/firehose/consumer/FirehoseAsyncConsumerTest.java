package io.odpf.firehose.consumer;

import io.odpf.firehose.consumer.kafka.ConsumerAndOffsetManager;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.firehose.sink.SinkPool;
import io.odpf.firehose.exception.SinkTaskFailedException;
import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.filter.FilteredMessages;
import io.odpf.firehose.filter.NoOpFilter;
import io.odpf.firehose.metrics.Metrics;
import io.odpf.firehose.tracer.SinkTracer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Future;

public class FirehoseAsyncConsumerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private SinkPool sinkPool;
    @Mock
    private SinkTracer tracer;
    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;
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
        FirehoseFilter firehoseFilter = new FirehoseFilter(new NoOpFilter(firehoseInstrumentation), firehoseInstrumentation);
        this.asyncConsumer = new FirehoseAsyncConsumer(sinkPool, tracer, consumerAndOffsetManager, firehoseFilter, firehoseInstrumentation);
    }

    @Test
    public void shouldPushMessagesInParallel() throws FilterException {
        List<Message> messageList1 = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
            add(new Message(new byte[0], new byte[0], "topic1", 2, 11));
            add(new Message(new byte[0], new byte[0], "topic1", 3, 12));
        }};

        List<Message> messageList2 = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 2, 5));
            add(new Message(new byte[0], new byte[0], "topic1", 2, 6));
        }};

        Mockito.when(consumerAndOffsetManager.readMessages()).thenReturn(messageList1);
        Mockito.when(sinkPool.submitTask(messageList1)).thenReturn(future1);
        Mockito.when(sinkPool.submitTask(messageList2)).thenReturn(future2);
        Mockito.when(sinkPool.fetchFinishedSinkTasks()).thenReturn(new HashSet<>());
        Mockito.when(future1.isDone()).thenReturn(false);
        Mockito.when(future2.isDone()).thenReturn(false);
        asyncConsumer.process();
        Mockito.when(consumerAndOffsetManager.readMessages()).thenReturn(messageList2);
        asyncConsumer.process();
        Mockito.verify(consumerAndOffsetManager, Mockito.times(1)).addOffsets(future1, messageList1);
        Mockito.verify(consumerAndOffsetManager, Mockito.times(1)).addOffsets(future2, messageList2);
        Mockito.verify(consumerAndOffsetManager, Mockito.times(0)).setCommittable(Mockito.any());
    }

    @Test
    public void shouldCallSetCommittableForDoneFutures() {
        List<Message> messages = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 11));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 12));
        }};
        Mockito.when(consumerAndOffsetManager.readMessages()).thenReturn(messages);

        Mockito.when(sinkPool.submitTask(messages)).thenReturn(future1);
        Mockito.when(sinkPool.fetchFinishedSinkTasks()).thenReturn(new HashSet<Future<List<Message>>>() {{
            add(future1);
        }});
        asyncConsumer.process();

        Mockito.verify(consumerAndOffsetManager, Mockito.times(1)).addOffsets(future1, messages);
        Mockito.verify(consumerAndOffsetManager, Mockito.times(1)).setCommittable(future1);
        Mockito.verify(consumerAndOffsetManager, Mockito.times(0)).forceAddOffsetsAndSetCommittable(new ArrayList<>());
        Mockito.verify(consumerAndOffsetManager, Mockito.times(1)).commit();
    }

    @Test
    public void shouldThrowExceptionIfSinkTaskFails() {
        expectedException.expect(SinkTaskFailedException.class);
        List<Message> messages = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 11));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 12));
        }};
        Mockito.when(consumerAndOffsetManager.readMessages()).thenReturn(messages);
        Mockito.when(sinkPool.submitTask(messages)).thenReturn(future1);
        Mockito.when(sinkPool.fetchFinishedSinkTasks()).thenThrow(new SinkTaskFailedException(new RuntimeException()));
        asyncConsumer.process();
    }

    @Test
    public void shouldAddOffsetsForFilteredMessages() throws Exception {
        FirehoseFilter firehoseFilter = Mockito.mock(FirehoseFilter.class);
        this.asyncConsumer = new FirehoseAsyncConsumer(sinkPool, tracer, consumerAndOffsetManager, firehoseFilter, firehoseInstrumentation);

        List<Message> messages = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 11));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 12));
        }};

        Mockito.when(consumerAndOffsetManager.readMessages()).thenReturn(messages);
        Mockito.when(firehoseFilter.applyFilter(messages)).thenReturn(new FilteredMessages() {{
            addToValidMessages(messages.get(0));
            addToInvalidMessages(messages.get(1));
            addToInvalidMessages(messages.get(2));
        }});
        Mockito.when(sinkPool.submitTask(new ArrayList<Message>() {{
            add(messages.get(0));
        }})).thenReturn(future1);
        Mockito.when(sinkPool.fetchFinishedSinkTasks()).thenReturn(new HashSet<Future<List<Message>>>() {{
            add(future1);
        }});
        asyncConsumer.process();

        Mockito.verify(consumerAndOffsetManager, Mockito.times(1)).addOffsets(future1, new ArrayList<Message>() {{
            add(messages.get(0));
        }});
        Mockito.verify(consumerAndOffsetManager, Mockito.times(1)).forceAddOffsetsAndSetCommittable(new ArrayList<Message>() {{
            add(messages.get(1));
            add(messages.get(2));
        }});
        Mockito.verify(consumerAndOffsetManager, Mockito.times(1)).setCommittable(future1);
        Mockito.verify(consumerAndOffsetManager, Mockito.times(1)).commit();
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).captureDurationSince(Mockito.eq(Metrics.SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS), Mockito.any(Instant.class));
    }
}
