package io.odpf.firehose.consumer;

import io.odpf.firehose.metrics.Instrumentation;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collection;

/**
 * A callback to log when the partition rebalancing happens.
 */
@AllArgsConstructor
public class ConsumerRebalancer implements ConsumerRebalanceListener {

    private Instrumentation instrumentation;

    /**
     * Function to run On partitions revoked.
     *
     * @param partitions list of partitions
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        instrumentation.logWarn("Partitions Revoked {}", Arrays.toString(partitions.toArray()));
    }

    /**
     * Function to run On partitions assigned.
     *
     * @param partitions list of partitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        instrumentation.logInfo("Partitions Assigned {}", Arrays.toString(partitions.toArray()));
    }
}
