package io.odpf.firehose.consumer;

import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.filter.Filter;
import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.metrics.Instrumentation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GenericConsumerTest {
    @Mock
    private KafkaConsumer kafkaConsumer;

    @Mock
    private ConsumerRecords consumerRecords;

    @Mock
    private Offsets offsets;

    @Mock
    private Filter filter;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private KafkaConsumerConfig consumerConfig;

    private TestMessage message;

    private TestKey key;

    private GenericConsumer genericConsumer;

    @Before
    public void setUp() {
        message = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();
        key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        genericConsumer = new GenericConsumer(kafkaConsumer, consumerConfig, filter, offsets, instrumentation);
        when(consumerConfig.getSourceKafkaPollTimeoutMs()).thenReturn(500L);
        when(kafkaConsumer.poll(Duration.ofMillis(500L))).thenReturn(consumerRecords);
    }

    @Test
    public void getsMessagesFromEsbLog() throws FilterException {
        ConsumerRecord<byte[], byte[]> record1 = new ConsumerRecord<>("topic1", 1, 0, key.toByteArray(), message.toByteArray());
        ConsumerRecord<byte[], byte[]> record2 = new ConsumerRecord<>("topic2", 1, 0, key.toByteArray(), message.toByteArray());
        when(consumerRecords.iterator()).thenReturn(Arrays.asList(record1, record2).iterator());

        Message expectedMsg1 = new Message(key.toByteArray(), message.toByteArray(), "topic1", 0, 100);
        Message expectedMsg2 = new Message(key.toByteArray(), message.toByteArray(), "topic2", 0, 100);

        when(filter.filter(any())).thenReturn(Arrays.asList(expectedMsg1, expectedMsg2));

        when(consumerRecords.count()).thenReturn(2);
        List<Message> messages = genericConsumer.readMessages();

        assertNotNull(messages);
        assertThat(messages.size(), is(2));
        assertEquals(expectedMsg1, messages.get(0));
        assertEquals(expectedMsg2, messages.get(1));
    }

    @Test
    public void getsMessagesFromEsbLogWithHeadersIfKafkaHeadersAreSet() throws FilterException {
        Headers headers = new RecordHeaders();
        ConsumerRecord<byte[], byte[]> record1 = new ConsumerRecord<>("topic1", 1, 0, 0, TimestampType.CREATE_TIME, 0L, 0, 0, key.toByteArray(), message.toByteArray(), headers);
        ConsumerRecord<byte[], byte[]> record2 = new ConsumerRecord<>("topic2", 1, 0, 0, TimestampType.CREATE_TIME, 0L, 0, 0, key.toByteArray(), message.toByteArray(), headers);
        when(consumerRecords.iterator()).thenReturn(Arrays.asList(record1, record2).iterator());

        Message expectedMsg1 = new Message(key.toByteArray(), message.toByteArray(), "topic1", 0, 100, headers, 1L, 1L);
        Message expectedMsg2 = new Message(key.toByteArray(), message.toByteArray(), "topic2", 0, 100, headers, 1L, 1L);

        when(filter.filter(any())).thenReturn(Arrays.asList(expectedMsg1, expectedMsg2));

        List<Message> messages = genericConsumer.readMessages();

        assertNotNull(messages);
        assertThat(messages.size(), is(2));
        assertEquals(expectedMsg1, messages.get(0));
        assertEquals(expectedMsg2, messages.get(1));
    }

    @Test
    public void getsFilteredMessagesFromEsbLog() throws FilterException {
        ConsumerRecord<byte[], byte[]> record1 = new ConsumerRecord<>("topic1", 1, 0, key.toByteArray(), message.toByteArray());
        ConsumerRecord<byte[], byte[]> record2 = new ConsumerRecord<>("topic2", 1, 0, key.toByteArray(), message.toByteArray());
        when(consumerRecords.iterator()).thenReturn(Arrays.asList(record1, record2).iterator());
        when(filter.getFilterRule()).thenReturn("test");

        Message expectedMsg1 = new Message(key.toByteArray(), message.toByteArray(), "topic1", 0, 100);

        when(filter.filter(any())).thenReturn(Arrays.asList(expectedMsg1));

        List<Message> messages = genericConsumer.readMessages();

        assertNotNull(messages);
        assertThat(messages.size(), is(1));
        assertEquals(expectedMsg1, messages.get(0));
        verify(instrumentation, times(1)).captureFilteredMessageCount(1, "test");
    }

    @Test
    public void shouldrecordStatsFromEsbLog() throws FilterException {
        ConsumerRecord<byte[], byte[]> record1 = new ConsumerRecord<>("topic1", 1, 0, key.toByteArray(), message.toByteArray());
        ConsumerRecord<byte[], byte[]> record2 = new ConsumerRecord<>("topic2", 1, 0, key.toByteArray(), message.toByteArray());
        when(consumerRecords.iterator()).thenReturn(Arrays.asList(record1, record2).iterator());

        Message expectedMsg1 = new Message(key.toByteArray(), message.toByteArray(), "topic1", 0, 100);
        Message expectedMsg2 = new Message(key.toByteArray(), message.toByteArray(), "topic2", 0, 100);

        when(filter.filter(any())).thenReturn(Arrays.asList(expectedMsg1, expectedMsg2));

        when(consumerRecords.count()).thenReturn(2);
        List<Message> messages = genericConsumer.readMessages();

        verify(instrumentation, times(1)).logInfo("Pulled {} messages", 2);
        verify(instrumentation, times(1)).capturePulledMessageHistogram(2);
        verify(instrumentation, times(1)).logDebug("Pulled record: {}", record1);
        verify(instrumentation, times(1)).logDebug("Pulled record: {}", record2);
    }

    @Test
    public void shouldCallCommitOnOffsets() {
        genericConsumer.commit();

        verify(offsets, times(1)).commit(any());
    }

    @Test
    public void shouldCallCloseOnConsumer() {
        genericConsumer.close();

        verify(kafkaConsumer).close();
        verify(instrumentation).logInfo("Consumer is closing");
    }

    @Test
    public void shouldSuppressExceptionOnClose() {
        doThrow(new RuntimeException()).when(kafkaConsumer).close();

        try {
            genericConsumer.close();
            verify(instrumentation, times(1)).logInfo("Consumer is closing");
        } catch (Exception kafkaConsumerException) {
            fail("Failed to supress exception on close");
        }
    }

    @Test
    public void shouldCaptureNonFatalError() {
        doThrow(new RuntimeException()).when(kafkaConsumer).close();
        genericConsumer.close();
        verify(instrumentation, times(1)).captureNonFatalError(any(), eq("Exception while closing consumer"));
    }
}
