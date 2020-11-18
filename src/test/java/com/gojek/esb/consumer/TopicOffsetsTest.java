package com.gojek.esb.consumer;

import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.metrics.Instrumentation;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TopicOffsetsTest {

    private TopicOffsets topicOffsets;

    @Mock
    private KafkaConsumer kafkaConsumer;

    @Mock
    private KafkaConsumerConfig consumerConfig;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private ConsumerRecords<byte[], byte[]> consumerRecords;

    @Before
    public void setup() {
        initMocks(this);

        topicOffsets = new TopicOffsets(kafkaConsumer, consumerConfig, instrumentation);
    }

    @Test
    public void shouldCommitSyncForAllPartitions() {
        when(consumerConfig.asyncCommitEnabled()).thenReturn(false);

        topicOffsets.commit(consumerRecords);

        verify(kafkaConsumer, times(1)).commitSync();
    }

    @Test
    public void shouldAsyncCommitForAllPartitions() {
        when(consumerConfig.asyncCommitEnabled()).thenReturn(true);

        topicOffsets.commit(consumerRecords);

        verify(kafkaConsumer, times(1)).commitAsync(any());
    }
}
