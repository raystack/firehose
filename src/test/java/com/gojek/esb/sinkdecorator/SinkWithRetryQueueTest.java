package com.gojek.esb.sinkdecorator;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.consumer.TestKey;
import com.gojek.esb.consumer.TestMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class SinkWithRetryQueueTest {

    @Mock
    private KafkaProducer<byte[], byte[]> kafkaProducer;

    @Mock
    private SinkWithRetry sinkWithRetry;

    @Mock
    private BackOffProvider backOffProvider;

    @Mock
    private EsbMessage esbMessage;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private StatsDReporter statsDReporter;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnEmptyListIfSuperReturnsEmptyList() throws IOException, DeserializerException {
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(new ArrayList<>());
        SinkWithRetryQueue sinkWithRetryQueue = SinkWithRetryQueue.withInstrumentationFactory(sinkWithRetry, kafkaProducer, "test-topic",
                statsDReporter, backOffProvider);
        ArrayList<EsbMessage> esbMessages = new ArrayList<>();
        esbMessages.add(esbMessage);
        List<EsbMessage> messages = sinkWithRetryQueue.pushMessage(esbMessages);

        assertTrue(messages.isEmpty());
        verifyZeroInteractions(kafkaProducer);
    }

    @Test
    public void shouldPublishToKafkaIfListIsNotEmpty() throws Exception {
        ArrayList<EsbMessage> esbMessages = new ArrayList<>();
        esbMessages.add(esbMessage);
        esbMessages.add(esbMessage);
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(esbMessages);

        SinkWithRetryQueue sinkWithRetryQueue = new SinkWithRetryQueue(sinkWithRetry, kafkaProducer, "test-topic",
                instrumentation, backOffProvider);
        Thread thread = new Thread(() -> {
            try {
                sinkWithRetryQueue.pushMessage(esbMessages);
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
    public void testRunShouldSendWithCorrectArgumentsIfHeadersAreNotSet() throws IOException, DeserializerException {
        ArrayList<EsbMessage> esbMessages = new ArrayList<>();
        esbMessages.add(esbMessage);
        esbMessages.add(esbMessage);
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(esbMessages);

        SinkWithRetryQueue sinkWithRetryQueue = new SinkWithRetryQueue(sinkWithRetry, kafkaProducer, "test-topic",
                instrumentation, backOffProvider);
        Thread thread = new Thread(() -> {
            try {
                sinkWithRetryQueue.pushMessage(esbMessages);
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

        assertEquals(expectedRecords(esbMessages.get(0)), actualRecords.get(0));
        assertEquals(expectedRecords(esbMessages.get(1)), actualRecords.get(1));
    }

    @Test
    public void testRunShouldSendWithCorrectArgumentsIfHeadersAreSet() throws IOException, DeserializerException {
        ArrayList<EsbMessage> esbMessages = new ArrayList<>();
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("key1", "value1".getBytes()));
        headers.add(new RecordHeader("key2", "value2".getBytes()));


        TestMessage message = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc")
                .setOrderDetails("details").build();
        TestKey key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();

        EsbMessage msg1 = new EsbMessage(key.toByteArray(), message.toByteArray(), "topic1", 0, 100, headers, 1L, 1L);
        EsbMessage msg2 = new EsbMessage(key.toByteArray(), message.toByteArray(), "topic2", 0, 100, headers, 1L, 1L);
        esbMessages.add(msg1);
        esbMessages.add(msg2);
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(esbMessages);

        SinkWithRetryQueue sinkWithRetryQueue = new SinkWithRetryQueue(sinkWithRetry, kafkaProducer, "test-topic",
                instrumentation, backOffProvider);
        Thread thread = new Thread(() -> {
            try {
                sinkWithRetryQueue.pushMessage(esbMessages);
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

        assertEquals(expectedRecords(esbMessages.get(0)), actualRecords.get(0));
        assertEquals(expectedRecords(esbMessages.get(1)), actualRecords.get(1));
    }

    @Test
    public void shouldRetryPublishToKafka() throws Exception {
        ArrayList<EsbMessage> esbMessages = new ArrayList<>();
        esbMessages.add(esbMessage);
        esbMessages.add(esbMessage);
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(esbMessages);

        SinkWithRetryQueue sinkWithRetryQueue = new SinkWithRetryQueue(sinkWithRetry, kafkaProducer, "test-topic",
                instrumentation, backOffProvider);
        Thread thread = new Thread(() -> {
            try {
                sinkWithRetryQueue.pushMessage(esbMessages);
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
        ArrayList<EsbMessage> esbMessages = new ArrayList<>();
        CountDownLatch completedLatch = new CountDownLatch(1);
        esbMessages.add(esbMessage);
        esbMessages.add(esbMessage);
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(esbMessages);

        SinkWithRetryQueue sinkWithRetryQueue = new SinkWithRetryQueue(sinkWithRetry, kafkaProducer, "test-topic",
                instrumentation, backOffProvider);
        Thread thread = new Thread(() -> {
            try {
                sinkWithRetryQueue.pushMessage(esbMessages);
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
        ArrayList<EsbMessage> esbMessages = new ArrayList<>();
        CountDownLatch completedLatch = new CountDownLatch(1);
        esbMessages.add(esbMessage);
        esbMessages.add(esbMessage);
        when(sinkWithRetry.pushMessage(anyList())).thenReturn(esbMessages);

        SinkWithRetryQueue sinkWithRetryQueue = new SinkWithRetryQueue(sinkWithRetry, kafkaProducer, "test-topic",
                instrumentation, backOffProvider);
        Thread thread = new Thread(() -> {
            try {
                sinkWithRetryQueue.pushMessage(esbMessages);
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

    private ProducerRecord<byte[], byte[]> expectedRecords(EsbMessage expectedMessage) {
        return new ProducerRecord<>("test-topic", null, null, expectedMessage.getLogKey(),
                expectedMessage.getLogMessage(), expectedMessage.getHeaders());
    }
}
