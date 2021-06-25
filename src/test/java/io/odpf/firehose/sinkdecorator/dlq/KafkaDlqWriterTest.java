package io.odpf.firehose.sinkdecorator.dlq;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.TestKey;
import io.odpf.firehose.consumer.TestMessage;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sinkdecorator.BackOffProvider;
import io.odpf.firehose.sinkdecorator.dlq.kafka.KafkaDlqWriter;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KafkaDlqWriterTest {

    @Mock
    private Message message;

    @Mock
    private KafkaProducer<byte[], byte[]> kafkaProducer;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private BackOffProvider backOffProvider;

    private KafkaDlqWriter kafkaDlqWriter;


    @Before
    public void setUp() throws Exception {
        kafkaDlqWriter = new KafkaDlqWriter(kafkaProducer, "test-topic", backOffProvider, instrumentation);
    }

    @Test
    public void shouldReturnEmptyListWhenWriteEmptyMessages() throws IOException {
        ArrayList<Message> messages = new ArrayList<>();
        List<Message> messageList = kafkaDlqWriter.write(messages);

        verifyZeroInteractions(kafkaProducer);
        assertTrue(messageList.isEmpty());
    }

    @Test
    public void shouldWriteToKafkaProducer() {
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);

        Thread thread = new Thread(() -> {
            try {
                kafkaDlqWriter.write(messages);
            } catch (IOException | DeserializerException e) {
                e.printStackTrace();
            }
        });

        thread.start();

        ArgumentCaptor<Callback> callbacks = ArgumentCaptor.forClass(Callback.class);
        verify(kafkaProducer, timeout(200).times(2)).send(any(), callbacks.capture());
        List<Callback> calls = callbacks.getAllValues();
        calls.get(0).onCompletion(null, null);
        calls.get(1).onCompletion(null, null);
    }

    @Test
    public void shouldSendWithCorrectArgumentsIfHeadersAreNotSet() {
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);

        Thread thread = new Thread(() -> {
            try {
                kafkaDlqWriter.write(messages);
            } catch (IOException | DeserializerException e) {
                e.printStackTrace();
            }
        });

        thread.start();

        ArgumentCaptor<Callback> callbacks = ArgumentCaptor.forClass(Callback.class);
        ArgumentCaptor<ProducerRecord> records = ArgumentCaptor.forClass(ProducerRecord.class);

        verify(kafkaProducer, timeout(200).times(2)).send(records.capture(), callbacks.capture());
        List<Callback> calls = callbacks.getAllValues();
        List<ProducerRecord> actualRecords = records.getAllValues();

        calls.get(0).onCompletion(null, null);
        calls.get(1).onCompletion(null, null);

        assertEquals(expectedRecords(messages.get(0)), actualRecords.get(0));
        assertEquals(expectedRecords(messages.get(1)), actualRecords.get(1));
    }

    private ProducerRecord<byte[], byte[]> expectedRecords(Message expectedMessage) {
        return new ProducerRecord<>("test-topic", null, null, expectedMessage.getLogKey(),
                expectedMessage.getLogMessage(), expectedMessage.getHeaders());
    }

    @Test
    public void shouldSendWithCorrectArgumentsIfHeadersAreSet() {
        ArrayList<Message> messages = new ArrayList<>();
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("key1", "value1".getBytes()));
        headers.add(new RecordHeader("key2", "value2".getBytes()));

        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc")
                .setOrderDetails("details").build();
        TestKey key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();

        Message msg1 = new Message(key.toByteArray(), testMessage.toByteArray(), "topic1", 0, 100, headers, 1L, 1L);
        Message msg2 = new Message(key.toByteArray(), testMessage.toByteArray(), "topic2", 0, 100, headers, 1L, 1L);
        messages.add(msg1);
        messages.add(msg2);

        Thread thread = new Thread(() -> {
            try {
                kafkaDlqWriter.write(messages);
            } catch (IOException | DeserializerException e) {
                e.printStackTrace();
            }
        });

        thread.start();

        ArgumentCaptor<Callback> callbacks = ArgumentCaptor.forClass(Callback.class);
        ArgumentCaptor<ProducerRecord> records = ArgumentCaptor.forClass(ProducerRecord.class);

        verify(kafkaProducer, timeout(200).times(2)).send(records.capture(), callbacks.capture());
        List<Callback> calls = callbacks.getAllValues();
        List<ProducerRecord> actualRecords = records.getAllValues();

        calls.get(0).onCompletion(null, null);
        calls.get(1).onCompletion(null, null);

        assertEquals(expectedRecords(messages.get(0)), actualRecords.get(0));
        assertEquals(expectedRecords(messages.get(1)), actualRecords.get(1));
    }

    @Test
    public void shouldRetryPublishToKafka() throws Exception {
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message);

        Thread thread = new Thread(() -> {
            try {
                kafkaDlqWriter.write(messages);
            } catch (IOException | DeserializerException e) {
                e.printStackTrace();
            }
        });

        thread.start();

        ArgumentCaptor<Callback> callbacks = ArgumentCaptor.forClass(Callback.class);
        verify(kafkaProducer, timeout(1000).times(2)).send(any(), callbacks.capture());
        List<Callback> calls = callbacks.getAllValues();
        calls.get(0).onCompletion(null, null);
        calls.get(1).onCompletion(null, new Exception());
        verify(kafkaProducer, timeout(200).times(3)).send(any(), callbacks.capture());
        calls = callbacks.getAllValues();
        calls.get(2).onCompletion(null, null);
    }


    @Test
    public void shouldRecordMessagesToRetryQueue() throws Exception {
        ArrayList<Message> messages = new ArrayList<>();
        CountDownLatch completedLatch = new CountDownLatch(1);
        messages.add(message);
        messages.add(message);

        Thread thread = new Thread(() -> {
            try {
                kafkaDlqWriter.write(messages);
                completedLatch.countDown();
            } catch (IOException | DeserializerException e) {
                e.printStackTrace();
            }
        });

        thread.start();

        ArgumentCaptor<Callback> callbacks = ArgumentCaptor.forClass(Callback.class);
        verify(kafkaProducer, timeout(200).times(2)).send(any(), callbacks.capture());
        List<Callback> calls = callbacks.getAllValues();
        calls.get(0).onCompletion(null, null);
        calls.get(1).onCompletion(null, null);
        completedLatch.await();
        verify(instrumentation, times(1)).captureRetryAttempts();
        verify(instrumentation, times(1)).logInfo("Pushing {} messages to retry queue topic : {}", 2, "test-topic");
        verify(instrumentation, times(2)).incrementMessageSucceedCount();
        verify(instrumentation, times(1)).logInfo("Successfully pushed {} messages to {}", 2, "test-topic");
    }

    @Test
    public void shouldRecordRetriesIfKafkaThrowsException() throws Exception {
        ArrayList<Message> messages = new ArrayList<>();
        CountDownLatch completedLatch = new CountDownLatch(1);
        messages.add(message);
        messages.add(message);

        Thread thread = new Thread(() -> {
            try {
                kafkaDlqWriter.write(messages);
                completedLatch.countDown();
            } catch (IOException | DeserializerException e) {
                e.printStackTrace();
            }
        });

        thread.start();
        Thread.sleep(1000L);

        ArgumentCaptor<Callback> callbacks = ArgumentCaptor.forClass(Callback.class);
        verify(kafkaProducer, timeout(1000).times(2)).send(any(), callbacks.capture());
        List<Callback> calls = callbacks.getAllValues();
        calls.get(0).onCompletion(null, null);
        calls.get(1).onCompletion(null, new Exception());
        verify(kafkaProducer, timeout(200).times(3)).send(any(), callbacks.capture());
        calls = callbacks.getAllValues();
        calls.get(2).onCompletion(null, null);
        completedLatch.await();

        verify(instrumentation, times(2)).captureRetryAttempts();
        verify(instrumentation, times(1)).logInfo("Pushing {} messages to retry queue topic : {}", 1, "test-topic");
        verify(instrumentation, times(1)).logInfo("Pushing {} messages to retry queue topic : {}", 2, "test-topic");
        verify(instrumentation, times(2)).incrementMessageSucceedCount();
        verify(instrumentation, times(1)).incrementMessageFailCount(any(), any());
        verify(instrumentation, times(2)).logInfo("Successfully pushed {} messages to {}", 1, "test-topic");
    }
}
