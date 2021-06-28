package io.odpf.firehose.sink.objectstorage;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.objectstorage.message.MessageDeSerializer;
import io.odpf.firehose.sink.objectstorage.message.Record;
import io.odpf.firehose.sink.objectstorage.writer.WriterOrchestrator;
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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ObjectStorageSinkTest {

    @Mock
    private WriterOrchestrator writerOrchestrator;
    private ObjectStorageSink objectStorageSink;
    @Mock
    private Instrumentation instrumentation;

    @Mock
    private MessageDeSerializer messageDeSerializer;

    @Before
    public void setUp() throws Exception {
        objectStorageSink = new ObjectStorageSink(instrumentation, "objectstorage", writerOrchestrator, messageDeSerializer);
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

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenWritingRecordThrowException() throws Exception {
        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        Record record1 = mock(Record.class);
        when(messageDeSerializer.deSerialize(message1)).thenReturn(record1);
        when(writerOrchestrator.write(record1)).thenThrow(new IOException(""));

        objectStorageSink.prepare(Arrays.asList(message1));
        objectStorageSink.execute();
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

        assertTrue(objectStorageSink.canManageOffsets());
        assertEquals(0, objectStorageSink.getCommittableOffsets().size());

        when(writerOrchestrator.getFlushedPaths()).thenReturn(new HashSet<String>() {{
            add(path1);
        }});
        Map<TopicPartition, OffsetAndMetadata> committableOffsets = objectStorageSink.getCommittableOffsets();
        assertEquals(1, committableOffsets.size());
        assertEquals(new OffsetAndMetadata(3), committableOffsets.get(new TopicPartition("booking", 1)));
    }
}
