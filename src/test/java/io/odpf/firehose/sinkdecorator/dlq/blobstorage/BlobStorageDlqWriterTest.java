package io.odpf.firehose.sinkdecorator.dlq.blobstorage;

import io.odpf.firehose.error.ErrorInfo;
import io.odpf.firehose.error.ErrorType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.blobstorage.BlobStorageException;
import io.odpf.firehose.blobstorage.BlobStorage;
import org.bson.internal.Base64;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class BlobStorageDlqWriterTest {

    @Mock
    private BlobStorage blobStorage;

    private BlobStorageDlqWriter blobStorageDLQWriter;

    @Before
    public void setUp() throws Exception {
        blobStorageDLQWriter = new BlobStorageDlqWriter(blobStorage);
    }

    @Test
    public void shouldWriteMessagesWithoutErrorInfoToObjectStorage() throws IOException, BlobStorageException {
        long timestamp1 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, timestamp1, timestamp1, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));
        Message message2 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 2, null, timestamp1, timestamp1, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        long timestamp2 = Instant.parse("2020-01-02T00:00:00Z").toEpochMilli();
        Message message3 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 3, null, timestamp2, timestamp2, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));
        Message message4 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 4, null, timestamp2, timestamp2, new ErrorInfo(new IOException("test"), ErrorType.DESERIALIZATION_ERROR));

        List<Message> messages = Arrays.asList(message1, message2, message3, message4);
        Assert.assertEquals(0, blobStorageDLQWriter.write(messages).size());

        String key = Base64.encode("123".getBytes());
        String message = Base64.encode("abc".getBytes());
        verify(blobStorage).store(contains("booking/2020-01-02"),
                eq(("{\"key\":\"" + key + "\",\"value\":\"" + message + "\",\"topic\":\"booking\",\"partition\":1,\"offset\":3,\"timestamp\":1577923200000,\"error\":\"DESERIALIZATION_ERROR\"}\n"
                        + "{\"key\":\"" + key + "\",\"value\":\"" + message + "\",\"topic\":\"booking\",\"partition\":1,\"offset\":4,\"timestamp\":1577923200000,\"error\":\"DESERIALIZATION_ERROR\"}").getBytes()));
        verify(blobStorage).store(contains("booking/2020-01-01"),
                eq(("{\"key\":\"" + key + "\",\"value\":\"" + message + "\",\"topic\":\"booking\",\"partition\":1,\"offset\":1,\"timestamp\":1577836800000,\"error\":\"DESERIALIZATION_ERROR\"}\n"
                        + "{\"key\":\"" + key + "\",\"value\":\"" + message + "\",\"topic\":\"booking\",\"partition\":1,\"offset\":2,\"timestamp\":1577836800000,\"error\":\"DESERIALIZATION_ERROR\"}").getBytes()));
    }

    @Test
    public void shouldWriteMessageErrorTypesToObjectStorage() throws IOException, BlobStorageException {
        long timestamp1 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, timestamp1, timestamp1, new ErrorInfo(new DeserializerException(""), ErrorType.DESERIALIZATION_ERROR));
        Message message2 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 2, null, timestamp1, timestamp1, new ErrorInfo(new NullPointerException(), ErrorType.SINK_UNKNOWN_ERROR));

        long timestamp2 = Instant.parse("2020-01-02T00:00:00Z").toEpochMilli();
        Message message3 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 3, null, timestamp2, timestamp2, new ErrorInfo(new DeserializerException(""), ErrorType.DESERIALIZATION_ERROR));
        Message message4 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 4, null, timestamp2, timestamp2, new ErrorInfo(new DeserializerException(""), ErrorType.SINK_UNKNOWN_ERROR));

        List<Message> messages = Arrays.asList(message1, message2, message3, message4);
        Assert.assertEquals(0, blobStorageDLQWriter.write(messages).size());

        String key = Base64.encode("123".getBytes());
        String message = Base64.encode("abc".getBytes());
        verify(blobStorage).store(contains("booking/2020-01-02"),
                eq(("{\"key\":\"" + key + "\",\"value\":\"" + message + "\",\"topic\":\"booking\",\"partition\":1,\"offset\":3,\"timestamp\":1577923200000,\"error\":\"DESERIALIZATION_ERROR\"}\n"
                        + "{\"key\":\"" + key + "\",\"value\":\"" + message + "\",\"topic\":\"booking\",\"partition\":1,\"offset\":4,\"timestamp\":1577923200000,\"error\":\"SINK_UNKNOWN_ERROR\"}").getBytes()));
        verify(blobStorage).store(contains("booking/2020-01-01"),
                eq(("{\"key\":\"" + key + "\",\"value\":\"" + message + "\",\"topic\":\"booking\",\"partition\":1,\"offset\":1,\"timestamp\":1577836800000,\"error\":\"DESERIALIZATION_ERROR\"}\n"
                        + "{\"key\":\"" + key + "\",\"value\":\"" + message + "\",\"topic\":\"booking\",\"partition\":1,\"offset\":2,\"timestamp\":1577836800000,\"error\":\"SINK_UNKNOWN_ERROR\"}").getBytes()));
    }

    @Test
    public void shouldThrowIOExceptionWhenWriteFileThrowIOException() throws IOException, BlobStorageException {
        long timestamp1 = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        Message message1 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 1, null, timestamp1, timestamp1, new ErrorInfo(new DeserializerException(""), ErrorType.DESERIALIZATION_ERROR));
        Message message2 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 2, null, timestamp1, timestamp1, new ErrorInfo(new DeserializerException(""), ErrorType.DESERIALIZATION_ERROR));

        long timestamp2 = Instant.parse("2020-01-02T00:00:00Z").toEpochMilli();
        Message message3 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 3, null, timestamp2, timestamp2, new ErrorInfo(new DeserializerException(""), ErrorType.DESERIALIZATION_ERROR));
        Message message4 = new Message("123".getBytes(), "abc".getBytes(), "booking", 1, 4, null, timestamp2, timestamp2, new ErrorInfo(new DeserializerException(""), ErrorType.DESERIALIZATION_ERROR));

        doThrow(new BlobStorageException("", "", new IOException())).when(blobStorage).store(anyString(), any(byte[].class));

        List<Message> messages = Arrays.asList(message1, message2, message3, message4);
        List<Message> failedMessages = blobStorageDLQWriter.write(messages);
        messages.sort(Comparator.comparingLong(Message::getOffset));
        failedMessages.sort(Comparator.comparingLong(Message::getOffset));
        Assert.assertEquals(messages, failedMessages);
    }
}
