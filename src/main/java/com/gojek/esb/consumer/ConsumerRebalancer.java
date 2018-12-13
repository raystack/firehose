package com.gojek.esb.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;

public class ConsumerRebalancer implements ConsumerRebalanceListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerRebalancer.class.getName());

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOGGER.info("Partitions Revoked {}", Arrays.toString(partitions.toArray()));
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOGGER.info("Partitions Assigned {}", Arrays.toString(partitions.toArray()));
    }
}
