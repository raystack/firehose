package io.odpf.firehose.consumer;

import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.Sink;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ConsumerAndOffsetManagerTest {


    private Message createMessage(String topic, int partition, int offset) {
        return new Message("".getBytes(), "".getBytes(), topic, partition, offset);
    }

    @Test
    public void shouldCommitToKafka() throws FilterException {
        Sink s1 = Mockito.mock(Sink.class);
        Sink s2 = Mockito.mock(Sink.class);
        Sink s3 = Mockito.mock(Sink.class);
        List<Sink> sinks = new ArrayList<Sink>() {{
            add(s1);
            add(s2);
            add(s3);
        }};
        GenericConsumer consumer = Mockito.mock(GenericConsumer.class);
        KafkaConsumerConfig config = ConfigFactory.create(KafkaConsumerConfig.class, new HashMap<>());
        Instrumentation instrumentation = Mockito.mock(Instrumentation.class);
        ConsumerAndOffsetManager consumerAndOffsetManager = new ConsumerAndOffsetManager(sinks, consumer, config, instrumentation);
        List<Message> messages = new ArrayList<Message>() {{
            add(createMessage("testing", 1, 1));
            add(createMessage("testing", 1, 2));
            add(createMessage("testing", 1, 3));
        }};
        Mockito.when(s1.canManageOffsets()).thenReturn(false);
        Mockito.when(consumer.readMessages()).thenReturn(messages);
        consumerAndOffsetManager.addOffsetsAndSetCommittable(consumerAndOffsetManager.readMessagesFromKafka());
        consumerAndOffsetManager.commit();
        Mockito.verify(consumer, Mockito.times(1)).commit(new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("testing", 1), new OffsetAndMetadata(4));
        }});
    }

    @Test
    public void shouldCommitToKafkaWithSinkManagesOwnOffsets() throws FilterException {
        Sink s1 = Mockito.mock(Sink.class);
        Sink s2 = Mockito.mock(Sink.class);
        Sink s3 = Mockito.mock(Sink.class);
        List<Sink> sinks = new ArrayList<Sink>() {{
            add(s1);
            add(s2);
            add(s3);
        }};
        List<Message> messages = new ArrayList<Message>() {{
            add(createMessage("testing", 1, 1));
            add(createMessage("testing", 1, 2));
            add(createMessage("testing", 1, 3));
        }};
        Instrumentation instrumentation = Mockito.mock(Instrumentation.class);
        GenericConsumer consumer = Mockito.mock(GenericConsumer.class);
        KafkaConsumerConfig config = ConfigFactory.create(KafkaConsumerConfig.class, new HashMap<>());

        Mockito.when(s1.canManageOffsets()).thenReturn(true);
        Mockito.when(s2.canManageOffsets()).thenReturn(true);
        Mockito.when(s3.canManageOffsets()).thenReturn(true);
        Mockito.when(consumer.readMessages()).thenReturn(messages);
        Mockito.when(s1.getCommittableOffsets()).thenReturn(new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("testing", 1), new OffsetAndMetadata(2));
        }});
        Mockito.when(s2.getCommittableOffsets()).thenReturn(new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("testing", 1), new OffsetAndMetadata(3));
        }});
        Mockito.when(s3.getCommittableOffsets()).thenReturn(new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("testing", 1), new OffsetAndMetadata(4));
        }});

        ConsumerAndOffsetManager consumerAndOffsetManager = new ConsumerAndOffsetManager(sinks, consumer, config, instrumentation);
        consumerAndOffsetManager.addOffsetsAndSetCommittable(consumerAndOffsetManager.readMessagesFromKafka());
        consumerAndOffsetManager.commit();
        Mockito.verify(consumer, Mockito.times(1)).commit(new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("testing", 1), new OffsetAndMetadata(2));
        }});
        Mockito.verify(consumer, Mockito.times(1)).commit(new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("testing", 1), new OffsetAndMetadata(3));
        }});
        Mockito.verify(consumer, Mockito.times(1)).commit(new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("testing", 1), new OffsetAndMetadata(4));
        }});
    }
}
