package io.odpf.firehose.sinkdecorator.dlq.objectstorage;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.objectstorage.ObjectStorage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ObjectStorageDlqWriterTest {

    @Mock
    private ObjectStorage objectStorage;

    private ObjectStorageDlqWriter objectStorageDLQWriter;

    @Before
    public void setUp() throws Exception {
        objectStorageDLQWriter = new ObjectStorageDlqWriter(objectStorage);
    }

    @Test
    public void shouldWriteMessagesToObjectStorage() throws IOException {
        long timestamp1 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1, null, 0, timestamp1);
        Message message2 = new Message("".getBytes(), "".getBytes(), "booking", 1, 2, null, 0, timestamp1);

        long timestamp2 = Instant.parse("2020-01-02T00:00:00Z").toEpochMilli();
        Message message3 = new Message("".getBytes(), "".getBytes(), "booking", 1, 3, null, 0, timestamp2);
        Message message4 = new Message("".getBytes(), "".getBytes(), "booking", 1, 4, null, 0, timestamp2);

        List<Message> messages = Arrays.asList(message1, message2, message3, message4);
        objectStorageDLQWriter.write(messages);

        verify(objectStorage).store(contains("booking/2020-01-02"),
                eq(("{\"key\":[],\"value\":[],\"topic\":\"booking\",\"partition\":1,\"offset\":3,\"timestamp\":1577923200000}\n"
                        + "{\"key\":[],\"value\":[],\"topic\":\"booking\",\"partition\":1,\"offset\":4,\"timestamp\":1577923200000}").getBytes()));
        verify(objectStorage).store(contains("booking/2020-01-01"),
                eq(("{\"key\":[],\"value\":[],\"topic\":\"booking\",\"partition\":1,\"offset\":1,\"timestamp\":1577836800000}\n"
                        + "{\"key\":[],\"value\":[],\"topic\":\"booking\",\"partition\":1,\"offset\":2,\"timestamp\":1577836800000}").getBytes()));
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenWriteFileThrowIOException() throws IOException {
        long timestamp1 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("".getBytes(), "".getBytes(), "booking", 1, 1, null, 0, timestamp1);
        Message message2 = new Message("".getBytes(), "".getBytes(), "booking", 1, 2, null, 0, timestamp1);

        long timestamp2 = Instant.parse("2020-01-02T00:00:00Z").toEpochMilli();
        Message message3 = new Message("".getBytes(), "".getBytes(), "booking", 1, 3, null, 0, timestamp2);
        Message message4 = new Message("".getBytes(), "".getBytes(), "booking", 1, 4, null, 0, timestamp2);

        doThrow(new IOException()).when(objectStorage).store(anyString(), any());

        List<Message> messages = Arrays.asList(message1, message2, message3, message4);
        objectStorageDLQWriter.write(messages);
    }
}
