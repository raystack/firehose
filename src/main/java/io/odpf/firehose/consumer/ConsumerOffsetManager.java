package io.odpf.firehose.consumer;

import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.consumer.offset.OffsetManager;
import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.Sink;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.util.List;

/**
 * This class have APIs to read from kafka and also provide offset management.
 * There are 2 use cases for this class.
 * 1. FirehoseConsumer:
 * consumerOffsetManager.readMessagesFromKafka(); // Read messages from kafka.
 * consumerOffsetManager.addOffsetsAndSetCommittable(messages); // add offsets for messages.
 * consumerOffsetManager.commit(); // commit all committable offsets for all partitions.
 * <p>
 * 2. FirehoseAsyncConsumer:
 * consumerOffsetManager.readMessagesFromKafka();
 * consumerOffsetManager.addOffsets(key, messages); or consumerOffsetManager.addPartitionedOffsets(key, messages)
 * consumerOffsetManager.setCommittable(key);
 * consumerOffsetManager.commit();
 * <p>
 * consumerOffsetManager.commit() commits offsets returned from sink if the sink can manages its own offsets
 * otherwise it commits offsets added to this class.
 */
@AllArgsConstructor
public class ConsumerOffsetManager implements AutoCloseable {
    private static final String SYNC_BATCH_KEY = "sync_batch_key";
    private final OffsetManager manager = new OffsetManager();
    private final Sink sink;
    private final GenericConsumer consumer;
    private final KafkaConsumerConfig consumerConfig;
    private final Instrumentation instrumentation;

    public void addPartitionedOffsets(Object key, List<Message> messages) {
        manager.addOffsetToBatchForLastMessage(key, messages);
    }

    public void addOffsets(Object key, List<Message> messages) {
        manager.addOffsetToBatch(key, messages);
    }


    public void addOffsetsAndSetCommittable(List<Message> messages) {
        if (!sink.canManageOffsets()) {
            addOffsets(SYNC_BATCH_KEY, messages);
            setCommittable(SYNC_BATCH_KEY);
        }
    }

    public void setCommittable(Object key) {
        manager.setCommittable(key);
    }

    public List<Message> readMessagesFromKafka() throws FilterException {
        return consumer.readMessages();
    }

    public void commit() {
        if (consumerConfig.isSourceKafkaCommitOnlyCurrentPartitionsEnable()) {
            consumer.commit(sink.canManageOffsets() ? sink.getCommittableOffsets() : manager.getCommittableOffset());
        } else {
            consumer.commit();
        }
    }

    @Override
    public void close() throws IOException {
        if (consumer != null) {
            instrumentation.logInfo("closing consumer");
            consumer.close();
        }
    }

}
