package io.odpf.firehose.consumer;

import io.odpf.firehose.metrics.Instrumentation;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collection;

@AllArgsConstructor
public class ConsumerRebalancer implements ConsumerRebalanceListener {

    private Instrumentation instrumentation;

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        instrumentation.logWarn("Partitions Revoked {}", Arrays.toString(partitions.toArray()));
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        instrumentation.logInfo("Partitions Assigned {}", Arrays.toString(partitions.toArray()));
    }
}
