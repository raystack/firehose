package io.odpf.firehose.consumer.offset;

import io.odpf.firehose.consumer.Message;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 *
 */
public class OffsetManager {
    private final Map<Object, Set<OffsetNode>> batchOffsets = new ConcurrentHashMap<>();
    private final Map<TopicPartition, TreeSet<OffsetNode>> sortedOffsets = new ConcurrentHashMap<>();

    public void addOffsetToBatch(Object batch, Message message) {
        OffsetNode currentNode = new OffsetNode(
                new TopicPartition(message.getTopic(), message.getPartition()),
                new OffsetAndMetadata(message.getOffset() + 1));
        batchOffsets.computeIfAbsent(batch, x -> new HashSet<>()).add(currentNode);
        sortedOffsets.computeIfAbsent(
                currentNode.getTopicPartition(),
                x -> new TreeSet<>(Comparator.comparingLong(node -> node.getOffsetAndMetadata().offset()))).add(currentNode);
    }

    public Set<OffsetNode> getOffsetsForBatch(Object key) {
        return batchOffsets.get(key);
    }

    public void commitBatch(Object batch) {
        batchOffsets.get(batch).forEach(x -> x.setCommittable(true));
        batchOffsets.remove(batch);
    }

    public TreeSet<OffsetNode> getOffsetsForTopicPartition(TopicPartition topicPartition) {
        return sortedOffsets.get(topicPartition);
    }

    public Map<TopicPartition, OffsetAndMetadata> getCommittableOffset() {
        return sortedOffsets.entrySet().stream().collect(
                Collectors.toMap(
                        Map.Entry::getKey,
                        kv -> compactAndFetchFirstCommittableNode(kv.getValue())
                )).entrySet().stream().filter(kv -> kv.getValue().isPresent()).collect(
                Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get().getOffsetAndMetadata()));
    }

    public Optional<OffsetNode> compactAndFetchFirstCommittableNode(TreeSet<OffsetNode> nodes) {
        if (nodes.size() == 0) {
            return Optional.empty();
        }
        Iterator<OffsetNode> iterator = nodes.iterator();
        OffsetNode current = null;
        OffsetNode previous;
        while (iterator.hasNext()) {
            previous = current;
            current = iterator.next();
            if (!current.isCommittable()) {
                break;
            }
            if (previous != null) {
                previous.setRemovable(true);
            }
        }
        nodes.removeIf(OffsetNode::isRemovable);
        return nodes.first().isCommittable() ? Optional.of(nodes.first()) : Optional.empty();
    }
}
