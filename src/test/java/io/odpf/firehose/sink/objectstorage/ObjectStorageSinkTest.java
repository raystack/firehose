package io.odpf.firehose.sink.objectstorage;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.offset.OffsetManager;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.objectstorage.message.MessageDeSerializer;
import io.odpf.firehose.sink.objectstorage.message.Record;
import io.odpf.firehose.sink.objectstorage.writer.WriterOrchestrator;
import io.odpf.firehose.sinkdecorator.dlq.DlqWriter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ObjectStorageSinkTest {

    @Mock
    private WriterOrchestrator writerOrchestrator;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private MessageDeSerializer messageDeSerializer;

    @Mock
    private DlqWriter dlqWriter;

    private ObjectStorageSink objectStorageSink;

    @Before
    public void setUp() throws Exception {
        objectStorageSink = new ObjectStorageSink(instrumentation, "objectstorage", writerOrchestrator, messageDeSerializer, dlqWriter);
    }

    @Test
    public void shouldWriteRecords() throws Exception {
        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        Message message2 = new Message("".getBytes(), "".getBytes(), "booking", 1, 2);
        Record record1 = mock(Record.class);
        Record record2 = mock(Record.class);
        String path1 = "/tmp/test1";
        String path2 = "/tmp/test2";

        when(messageDeSerializer.deSerialize(message1)).thenReturn(record1);
        when(messageDeSerializer.deSerialize(message2)).thenReturn(record2);
        when(writerOrchestrator.write(record1)).thenReturn(path1);
        when(writerOrchestrator.write(record2)).thenReturn(path2);

        List<Message> retryMessages = objectStorageSink.pushMessage(Arrays.asList(message1, message2));

        verify(writerOrchestrator, times(2)).write(any(Record.class));
        assertEquals(0, retryMessages.size());
    }

    @Test(expected = Exception.class)
    public void shouldThrowExceptionWhenWritingRecordThrowIOException() throws Exception {
        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        Record record1 = mock(Record.class);
        when(messageDeSerializer.deSerialize(message1)).thenReturn(record1);
        when(writerOrchestrator.write(record1)).thenThrow(new IOException("error"));

        objectStorageSink.pushMessage(Arrays.asList(message1));
    }

    @Test
    public void shouldReturnCommittableOffsets() throws Exception {
        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        Message message2 = new Message("".getBytes(), "".getBytes(), "booking", 1, 2);
        Record record1 = mock(Record.class);
        Record record2 = mock(Record.class);
        String path1 = "/tmp/test1";

        when(messageDeSerializer.deSerialize(message1)).thenReturn(record1);
        when(messageDeSerializer.deSerialize(message2)).thenReturn(record2);
        when(writerOrchestrator.write(record1)).thenReturn(path1);
        when(writerOrchestrator.write(record2)).thenReturn(path1);
        when(writerOrchestrator.getFlushedPaths()).thenReturn(new HashSet<>());
        objectStorageSink.pushMessage(Arrays.asList(message1, message2));

        assertFalse(objectStorageSink.canSyncCommit());
        assertEquals(0, objectStorageSink.getCommittableOffset().size());

        when(writerOrchestrator.getFlushedPaths()).thenReturn(new HashSet<String>() {{
            add(path1);
        }});
        Map<TopicPartition, OffsetAndMetadata> committableOffsets = objectStorageSink.getCommittableOffset();
        assertEquals(1, committableOffsets.size());
        assertEquals(new OffsetAndMetadata(3), committableOffsets.get(new TopicPartition("booking", 1)));
    }

    @Test
    public void shouldHandleInvalidMessageToDLQ() throws Exception {
        OffsetManager offsetManager = mock(OffsetManager.class);
        objectStorageSink = new ObjectStorageSink(instrumentation, "objectstorage", writerOrchestrator, messageDeSerializer, offsetManager, dlqWriter);

        TopicPartition topicPartition1 = new TopicPartition("booking", 1);
        TopicPartition topicPartition2 = new TopicPartition("booking", 2);

        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        Message message2 = new Message("".getBytes(), "".getBytes(), "booking", 1, 2);
        Message message3 = new Message("".getBytes(), "".getBytes(), "booking", 1, 3);
        Message message4 = new Message("".getBytes(), "".getBytes(), "booking", 1, 4);
        Message message5 = new Message("".getBytes(), "".getBytes(), "booking", 2, 1);
        Message message6 = new Message("".getBytes(), "".getBytes(), "booking", 2, 2);
        Record record1 = mock(Record.class);
        Record record2 = mock(Record.class);
        String path1 = "/tmp/test1";
        String path2 = "/tmp/test2";

        when(messageDeSerializer.deSerialize(message1)).thenReturn(record1);
        when(messageDeSerializer.deSerialize(message2)).thenReturn(record2);
        when(messageDeSerializer.deSerialize(message3)).thenThrow(new DeserializerException(""));
        when(messageDeSerializer.deSerialize(message4)).thenThrow(new DeserializerException(""));
        when(messageDeSerializer.deSerialize(message5)).thenThrow(new DeserializerException(""));
        when(messageDeSerializer.deSerialize(message6)).thenThrow(new DeserializerException(""));
        when(writerOrchestrator.write(record1)).thenReturn(path1);
        when(writerOrchestrator.write(record2)).thenReturn(path2);

        List<Message> retryMessages = objectStorageSink.pushMessage(Arrays.asList(message1, message2, message3, message4, message5, message6));

        verify(offsetManager).addOffsetToBatch(path1, message1);
        verify(offsetManager).addOffsetToBatch(path2, message2);
        verify(offsetManager).addOffsetToBatch(topicPartition1, message3);
        verify(offsetManager).addOffsetToBatch(topicPartition1, message4);
        verify(offsetManager).addOffsetToBatch(topicPartition2, message5);
        verify(offsetManager).addOffsetToBatch(topicPartition2, message6);
        verify(offsetManager).commitBatch(topicPartition1);
        verify(offsetManager).commitBatch(topicPartition2);
        verify(writerOrchestrator, times(2)).write(any(Record.class));
        assertEquals(0, retryMessages.size());
    }
}
