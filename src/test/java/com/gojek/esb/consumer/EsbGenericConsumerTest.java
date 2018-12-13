package com.gojek.esb.consumer;

import com.gojek.esb.audit.AuditMessage;
import com.gojek.esb.audit.AuditableProtoMessage;
import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.filter.EsbFilterException;
import com.gojek.esb.filter.Filter;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.server.AuditServiceClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EsbGenericConsumerTest {
    @Mock
    private KafkaConsumer kafkaConsumer;

    @Mock
    private ConsumerRecords consumerRecords;

    @Mock
    private AuditServiceClient auditServiceClient;

    @Mock
    private AuditMessage auditMessage;

    @Mock
    private Offsets offsets;

    @Mock
    private Filter filter;

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private KafkaConsumerConfig consumerConfig;

    private TestMessage message;

    private TestKey key;

    private EsbGenericConsumer consumerWithAudit;
    private ConsumerRecord<byte[], byte[]> consumerRecord1;
    private ConsumerRecord<byte[], byte[]> consumerRecord2;

    @Captor
    private ArgumentCaptor<Stream<AuditableProtoMessage>> auditMessageCaptor;

    @Before
    public void setUp() throws Exception {
        message = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();
        key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        consumerWithAudit = new EsbGenericConsumer(kafkaConsumer, consumerConfig, auditServiceClient, filter, offsets, statsDReporter);
        when(consumerConfig.getPollTimeOut()).thenReturn(500L);
        when(kafkaConsumer.poll(500L)).thenReturn(consumerRecords);
        consumerRecord1 = new ConsumerRecord<>("topic1", 1, 0, key.toByteArray(), message.toByteArray());
        consumerRecord2 = new ConsumerRecord<>("topic2", 1, 0, key.toByteArray(), message.toByteArray());
    }

    @Test
    public void getsMessagesFromEsbLog() throws EsbFilterException {
        ConsumerRecord<byte[], byte[]> record1 = new ConsumerRecord<>("topic1", 1, 0, key.toByteArray(), message.toByteArray());
        ConsumerRecord<byte[], byte[]> record2 = new ConsumerRecord<>("topic2", 1, 0, key.toByteArray(), message.toByteArray());
        when(consumerRecords.iterator()).thenReturn(Arrays.asList(record1, record2).iterator());

        EsbMessage expectedMsg1 = new EsbMessage(key.toByteArray(), message.toByteArray(), "topic1", 0, 100);
        EsbMessage expectedMsg2 = new EsbMessage(key.toByteArray(), message.toByteArray(), "topic2", 0, 100);

        when(filter.filter(any())).thenReturn(Arrays.asList(expectedMsg1, expectedMsg2));

        List<EsbMessage> messages = consumerWithAudit.readMessages();

        assertNotNull(messages);
        assertThat(messages.size(), is(2));
        assertEquals(expectedMsg1, messages.get(0));
        assertEquals(expectedMsg2, messages.get(1));
    }

    @Test
    public void getsFilteredMessagesFromEsbLog() throws EsbFilterException {
        ConsumerRecord<byte[], byte[]> record1 = new ConsumerRecord<>("topic1", 1, 0, key.toByteArray(), message.toByteArray());
        ConsumerRecord<byte[], byte[]> record2 = new ConsumerRecord<>("topic2", 1, 0, key.toByteArray(), message.toByteArray());
        when(consumerRecords.iterator()).thenReturn(Arrays.asList(record1, record2).iterator());
        when(consumerConfig.getFilterExpression()).thenReturn("test");

        EsbMessage expectedMsg1 = new EsbMessage(key.toByteArray(), message.toByteArray(), "topic1", 0, 100);

        when(filter.filter(any())).thenReturn(Arrays.asList(expectedMsg1));

        List<EsbMessage> messages = consumerWithAudit.readMessages();

        assertNotNull(messages);
        assertThat(messages.size(), is(1));
        assertEquals(expectedMsg1, messages.get(0));
        verify(statsDReporter, times(1)).captureCount("kafka.filtered", 1,  "expr=test");

    }

    @Test
    public void shouldAuditMessages() throws Exception {
        when(consumerRecords.iterator()).thenReturn(Arrays.asList(consumerRecord1, consumerRecord2).iterator());

        consumerWithAudit.readMessages();

        verify(auditServiceClient).sendAuditableMessages(auditMessageCaptor.capture());
        List<AuditableProtoMessage> actualMessages = auditMessageCaptor.getValue().collect(Collectors.toList());
        assertThat(actualMessages.get(0).getLogKey(), is(key.toByteArray()));
        assertThat(actualMessages.get(0).getTopic(), is("topic1"));
        assertThat(actualMessages.get(1).getLogKey(), is(key.toByteArray()));
        assertThat(actualMessages.get(1).getTopic(), is("topic2"));
    }

    @Test
    public void shouldNotAuditMessagesWithoutKey() throws Exception {
        ConsumerRecord<Object, byte[]> recordWithoutKey = new ConsumerRecord<>("topic2", 1, 0, null, message.toByteArray());
        when(consumerRecords.iterator()).thenReturn(Arrays.asList(consumerRecord1, recordWithoutKey).iterator());

        consumerWithAudit.readMessages();

        verify(auditServiceClient).sendAuditableMessages(auditMessageCaptor.capture());
        List<AuditableProtoMessage> expectedAuditPayload = Arrays.asList(new AuditableProtoMessage(key.toByteArray(), "topic1"));
        assertThat(auditMessageCaptor.getValue().collect(Collectors.toList()), is(expectedAuditPayload));
    }

    @Test
    public void shouldNotAuditWhenThereIsEmptyMessagesDuringRead() throws Exception {
        when(consumerRecords.iterator()).thenReturn(emptyList().iterator());

        consumerWithAudit.readMessages();

        verify(auditServiceClient, never()).sendAuditableMessages(any());
    }

    @Test
    public void shouldCallCommitOnOffsets() {
        consumerWithAudit.commit();

        verify(offsets, times(1)).commit(any());
    }

    @Test
    public void shouldCallCloseOnConsumer() {
        consumerWithAudit.close();

        verify(kafkaConsumer).close();
    }

    @Test
    public void shouldSuppressExceptionOnClose() {
        doThrow(new RuntimeException()).when(kafkaConsumer).close();

        try {
            consumerWithAudit.close();
        } catch (Exception kafkaConsumerException) {
            fail("Failed to supress exception on close");
        }
    }
}
