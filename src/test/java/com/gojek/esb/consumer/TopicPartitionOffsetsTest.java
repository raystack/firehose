package com.gojek.esb.consumer;

import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.metrics.Instrumentation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TopicPartitionOffsetsTest {

    private TopicPartitionOffsets offsets;

    @Mock
    private KafkaConsumer kafkaConsumer;

    @Mock
    private KafkaConsumerConfig consumerConfig;

    @Mock
    private Instrumentation instrumentation;

    private TestMessage message;
    private TestKey key;

    private Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records;
    private ConsumerRecord<byte[], byte[]> consumerRecord1;
    private ConsumerRecord<byte[], byte[]> consumerRecord2;
    private ConsumerRecords<byte[], byte[]> consumerRecords;
    private TopicPartition topicPartition;

    @Before
    public void setup() {
        initMocks(this);

        offsets = new TopicPartitionOffsets(kafkaConsumer, consumerConfig, instrumentation);

        message = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();
        key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        consumerRecord1 = new ConsumerRecord<>("topic1", 1, 0, key.toByteArray(), message.toByteArray());
        consumerRecord2 = new ConsumerRecord<>("topic1", 1, 1, key.toByteArray(), message.toByteArray());

        records = new HashMap<>();
        topicPartition = new TopicPartition(consumerRecord1.topic(), consumerRecord1.partition());

        records.put(topicPartition, Arrays.asList(consumerRecord2, consumerRecord1));
        consumerRecords = new ConsumerRecords<>(records);
    }

    @Test
    public void shouldCommitSyncForCurrentPartitions() {
        when(consumerConfig.isSourceKafkaAsyncCommitEnable()).thenReturn(false);

        offsets.commit(consumerRecords);

        verify(kafkaConsumer, times(1)).commitSync(any(Map.class));
    }

    @Test
    public void shouldAsyncCommitForCurrentPartitions() {
        when(consumerConfig.isSourceKafkaAsyncCommitEnable()).thenReturn(true);

        offsets.commit(consumerRecords);

        verify(kafkaConsumer, times(1)).commitAsync(any(Map.class), any());
    }

    @Test
    public void shouldCommitMaxOffsetInFetchedRecords() {
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = offsets.createOffsetsAndMetadata(consumerRecords);
        Assert.assertEquals(1, topicPartitionOffsetAndMetadataMap.size());
        Assert.assertEquals(2, topicPartitionOffsetAndMetadataMap.get(topicPartition).offset());
    }
}
