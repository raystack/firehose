package io.odpf.firehose.sink.file;

import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class FileSinkTest {

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private Serializer serializer;

    @Mock
    private FileWriter fileWriter;

    private FileSink fileSink;
    private Message message;
    private PathBuilder path;

    @Before
    public void setUp() throws Exception {
        path = new PathBuilder().setDir(Paths.get("")).setFileName("booking");
        fileSink = new FileSink(instrumentation, "file", fileWriter, serializer, path);
        message = new Message("key".getBytes(), "msg".getBytes(), "topic1", 0, 100);
    }

    @Test
    public void shouldSerialiseMessageIntoRecord() throws SQLException, IOException {
        Record record = new Record();
        when(serializer.serialize(message)).thenReturn(record);

        fileSink.prepare(Arrays.asList(message, message));
        verify(serializer, times(2)).serialize(message);
    }

    @Test(expected = DeserializerException.class)
    public void shouldReturnFailedMessagesWhenSerializeThrowsException() throws IOException, SQLException {
        DeserializerException exception = new DeserializerException("");
        when(serializer.serialize(message)).thenThrow(exception);

        fileSink.prepare(Arrays.asList(message));
    }


    @Test
    public void shouldWriteToFile() throws IOException {
        Record record = new Record();
        when(serializer.serialize(message)).thenReturn(record);
        doNothing().when(fileWriter).open(path);

        List<Message> messages = Arrays.asList(message, message);
        fileSink.pushMessage(messages);

        verify(fileWriter, times(2)).write(record);
    }

    @Test
    public void shouldReturnEmptyListWhenNoException() throws IOException {
        Record record = new Record();
        when(serializer.serialize(message)).thenReturn(record);
        doNothing().when(fileWriter).open(path);

        List<Message> messages = Arrays.asList(message, message);
        assertEquals(fileSink.pushMessage(messages).size(), 0);
    }


    @Test
    public void shouldReturnFailedMessagesWhenExecuteThrowsException() throws IOException {
        Record record = new Record();
        when(serializer.serialize(message)).thenReturn(record);
        doNothing().when(fileWriter).open(path);

        IOException exception = new IOException();
        doThrow(exception).when(fileWriter).write(record);

        List<Message> messages = Arrays.asList(message, message);
        assertEquals(fileSink.pushMessage(messages).size(), 2);
    }
}