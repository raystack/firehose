package io.odpf.firehose.sink.objectstorage;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.objectstorage.message.Record;
import io.odpf.firehose.sink.objectstorage.writer.WriterOrchestrator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ObjectStorageSinkTest {

    private final Path basePath = Paths.get("");
    @Mock
    private WriterOrchestrator writerOrchestrator;
    private ObjectStorageSink objectStorageSink;
    @Mock
    private Instrumentation instrumentation;

    @Before
    public void setUp() throws Exception {
        objectStorageSink = new ObjectStorageSink(instrumentation, "file", writerOrchestrator);
    }

    @Test
    public void shouldWriteRecords() throws IOException {
        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        Message message2 = new Message("".getBytes(), "".getBytes(), "booking", 1, 2);
        String pathToWriter = "/tmp/test";

        Record record1 = new Record(null, null);
        Record record2 = new Record(null, null);

        when(writerOrchestrator.convertToRecord(message1)).thenReturn(record1);
        when(writerOrchestrator.convertToRecord(message2)).thenReturn(record2);

        when(writerOrchestrator.write(any(Record.class))).thenReturn(pathToWriter);

        List<Message> retryMessages = objectStorageSink.pushMessage(Arrays.asList(message1, message2));

        verify(writerOrchestrator, times(2)).write(any(Record.class));
        assertEquals(0, retryMessages.size());
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenWritingRecordThrowException() throws IOException, SQLException {
        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        Record record = new Record(null, null);

        when(writerOrchestrator.convertToRecord(message1)).thenReturn(record);
        doThrow(new IOException("")).when(writerOrchestrator).write(record);

        objectStorageSink.prepare(Arrays.asList(message1));
        List<Message> messages = objectStorageSink.execute();
        assertEquals(1, messages.size());
    }

    @Test(expected = DeserializerException.class)
    public void shouldThrowDeserializerExceptionWhenSerialiseThrowException() throws SQLException, IOException {
        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        when(writerOrchestrator.convertToRecord(message1)).thenThrow(new DeserializerException(""));

        objectStorageSink.prepare(Arrays.asList(message1));
    }
}
