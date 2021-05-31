package io.odpf.firehose.sink.file;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.file.message.MessageSerializer;
import io.odpf.firehose.sink.file.message.Record;
import io.odpf.firehose.sink.file.writer.LocalParquetFileWriter;
import io.odpf.firehose.sink.file.writer.WriterOrchestrator;
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

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class FileSinkTest {

    @Mock
    private MessageSerializer messageSerializer;

    @Mock
    private LocalParquetFileWriter localParquetFileWriter;

    @Mock
    private WriterOrchestrator writerOrchestrator;

    private FileSink fileSink;

    @Mock
    private Instrumentation instrumentation;

    private Path basePath = Paths.get("");

    @Before
    public void setUp() throws Exception {
        fileSink = new FileSink(instrumentation, "file", writerOrchestrator, messageSerializer, basePath);
    }

    @Test
    public void shouldWriteRecords() throws IOException {
        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        Message message2 = new Message("".getBytes(), "".getBytes(), "booking", 1, 2);

        Record record1 = new Record(null, null);
        Record record2 = new Record(null, null);

        when(messageSerializer.serialize(message1)).thenReturn(record1);
        when(messageSerializer.serialize(message2)).thenReturn(record2);

        when(writerOrchestrator.getWriter(eq(basePath), any(Record.class))).thenReturn(localParquetFileWriter);

        List<Message> retryMessages = fileSink.pushMessage(Arrays.asList(message1, message2));

        verify(localParquetFileWriter, times(2)).write(any(Record.class));
        assertEquals(0, retryMessages.size());
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenWritingRecordThrowException() throws IOException, SQLException {
        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        Record record = new Record(null, null);

        when(messageSerializer.serialize(message1)).thenReturn(record);
        when(writerOrchestrator.getWriter(basePath, record)).thenReturn(localParquetFileWriter);
        doThrow(new IOException("")).when(localParquetFileWriter).write(record);

        fileSink.prepare(Arrays.asList(message1));
        List<Message> messages = fileSink.execute();
        assertEquals(1, messages.size());
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenCreatingWriterThrowException() throws IOException, SQLException {
        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        Record record = new Record(null, null);

        when(messageSerializer.serialize(message1)).thenReturn(record);
        when(writerOrchestrator.getWriter(basePath, record)).thenThrow(new IOException(""));

        fileSink.prepare(Arrays.asList(message1));
        List<Message> messages = fileSink.execute();
        assertEquals(1, messages.size());
    }

    @Test(expected = DeserializerException.class)
    public void shouldThrowDeserializerExceptionWhenSerialiseThrowException() throws SQLException, IOException {
        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        when(messageSerializer.serialize(message1)).thenThrow(new DeserializerException(""));

        fileSink.prepare(Arrays.asList(message1));
    }
}
