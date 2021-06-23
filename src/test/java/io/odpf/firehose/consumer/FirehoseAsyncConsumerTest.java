package io.odpf.firehose.consumer;

import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.sink.Sink;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class FirehoseAsyncConsumerTest {

    @Mock
    private ExecutorService executorService;
    @Mock
    private Sink sink;
    @Mock
    private Future<List<Message>> future1;
    @Mock
    private Future<List<Message>> future2;
    @Mock
    private ConsumerOffsetManager consumerOffsetManager;
    private FirehoseAsyncConsumer asyncConsumer;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        this.asyncConsumer = new FirehoseAsyncConsumer(sink, executorService, consumerOffsetManager);
    }

    @Test
    public void shouldPushMessagesInParallelForEachPartition() throws FilterException, IOException {
        Mockito.when(consumerOffsetManager.readMessagesFromKafka()).thenReturn(new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 11));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 12));

            add(new Message(new byte[0], new byte[0], "topic1", 2, 5));
            add(new Message(new byte[0], new byte[0], "topic1", 2, 6));
        }});

        List<Message> messagesFromPartition1 = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 11));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 12));
        }};
        List<Message> messagesFromPartition2 = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 2, 5));
            add(new Message(new byte[0], new byte[0], "topic1", 2, 6));
        }};

        Mockito.when(sink.pushMessage(messagesFromPartition1)).thenReturn(new ArrayList<>());
        Mockito.when(sink.pushMessage(messagesFromPartition2)).thenReturn(new ArrayList<>());

        Mockito.when(executorService.submit(new FirehoseAsyncConsumer.SinkTask(sink, messagesFromPartition1))).thenReturn(future1);
        Mockito.when(executorService.submit(new FirehoseAsyncConsumer.SinkTask(sink, messagesFromPartition2))).thenReturn(future2);
        Mockito.when(future1.isDone()).thenReturn(false);
        Mockito.when(future2.isDone()).thenReturn(false);
        asyncConsumer.process();

        Mockito.verify(consumerOffsetManager, Mockito.times(1)).addPartitionedOffsets(future1, messagesFromPartition1);
        Mockito.verify(consumerOffsetManager, Mockito.times(1)).addPartitionedOffsets(future2, messagesFromPartition2);
        Mockito.verify(consumerOffsetManager, Mockito.times(0)).setCommittable(Mockito.any());
    }

    @Test
    public void shouldCallSetCommittableForDoneFutures() throws FilterException, IOException {
        Mockito.when(consumerOffsetManager.readMessagesFromKafka()).thenReturn(new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 11));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 12));
        }});

        List<Message> messagesFromPartition1 = new ArrayList<Message>() {{
            add(new Message(new byte[0], new byte[0], "topic1", 1, 10));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 11));
            add(new Message(new byte[0], new byte[0], "topic1", 1, 12));
        }};

        Mockito.when(sink.pushMessage(messagesFromPartition1)).thenReturn(new ArrayList<>());

        Mockito.when(executorService.submit(new FirehoseAsyncConsumer.SinkTask(sink, messagesFromPartition1))).thenReturn(future1);
        Mockito.when(future1.isDone()).thenReturn(true);
        asyncConsumer.process();

        Mockito.verify(consumerOffsetManager, Mockito.times(1)).addPartitionedOffsets(future1, messagesFromPartition1);
        Mockito.verify(consumerOffsetManager, Mockito.times(1)).setCommittable(future1);
    }
}
