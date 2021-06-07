package io.odpf.firehose.sink.cloud;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.cloud.message.MessageSerializer;
import io.odpf.firehose.sink.cloud.message.Record;
import io.odpf.firehose.sink.cloud.writer.WriterOrchestrator;
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
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CloudSinkTest {

    @Mock
    private MessageSerializer messageSerializer;

    @Mock
    private WriterOrchestrator writerOrchestrator;

    private CloudSink cloudSink;

    @Mock
    private Instrumentation instrumentation;

    private final Path basePath = Paths.get("");

    @Before
    public void setUp() throws Exception {
        cloudSink = new CloudSink(instrumentation, "file", writerOrchestrator);
    }

    @Test
    public void shouldWriteRecords() throws IOException {
        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        Message message2 = new Message("".getBytes(), "".getBytes(), "booking", 1, 2);
        String pathToWriter = "/tmp/test";

        Record record1 = new Record(null, null);
        Record record2 = new Record(null, null);

        when(messageSerializer.serialize(message1)).thenReturn(record1);
        when(messageSerializer.serialize(message2)).thenReturn(record2);

        when(writerOrchestrator.getMessageSerializer()).thenReturn(messageSerializer);
        when(writerOrchestrator.write(any(Record.class))).thenReturn(pathToWriter);

        List<Message> retryMessages = cloudSink.pushMessage(Arrays.asList(message1, message2));

        verify(writerOrchestrator, times(2)).write(any(Record.class));
        assertEquals(0, retryMessages.size());
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenWritingRecordThrowException() throws IOException, SQLException {
        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        Record record = new Record(null, null);

        when(messageSerializer.serialize(message1)).thenReturn(record);
        when(writerOrchestrator.getMessageSerializer()).thenReturn(messageSerializer);
        doThrow(new IOException("")).when(writerOrchestrator).write(record);

        cloudSink.prepare(Arrays.asList(message1));
        List<Message> messages = cloudSink.execute();
        assertEquals(1, messages.size());
    }

    @Test(expected = DeserializerException.class)
    public void shouldThrowDeserializerExceptionWhenSerialiseThrowException() throws SQLException, IOException {
        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1);
        when(writerOrchestrator.getMessageSerializer()).thenReturn(messageSerializer);
        when(messageSerializer.serialize(message1)).thenThrow(new DeserializerException(""));

        cloudSink.prepare(Arrays.asList(message1));
    }
}
