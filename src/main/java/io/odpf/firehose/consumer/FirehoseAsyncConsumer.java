package io.odpf.firehose.consumer;

import io.odpf.firehose.consumer.offset.OffsetManager;
import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.sink.Sink;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class FirehoseAsyncConsumer implements KafkaConsumer {
    private final Sink sink;
    private final GenericConsumer consumer;
    private final ExecutorService executorService;
    private final OffsetManager manager = new OffsetManager();
    private final Set<Future<?>> futures = new HashSet<>();

    public FirehoseAsyncConsumer(Sink sink, GenericConsumer consumer, int sourceKafkaConsumerThreads) {
        this.sink = sink;
        this.consumer = consumer;
        this.executorService = Executors.newFixedThreadPool(sourceKafkaConsumerThreads);
    }

    @Override
    public void process() throws FilterException {
        List<Message> messages = consumer.readMessages();
        if (!messages.isEmpty()) {
            pushMessages(messages);
        }
        checkFinishedSinkTasks();
        if (sink.canSyncCommit()) {
            consumer.commit(manager.getCommittableOffset());
        } else {
            consumer.commit(sink.getCommittableOffset());
        }
    }

    private void checkFinishedSinkTasks() {
        futures.removeIf(f -> {
            if (!f.isDone()) {
                return false;
            }
            try {
                f.get();
            } catch (InterruptedException e) {
                throw new AsyncConsumerFailedException(e);
            } catch (ExecutionException e) {
                throw new AsyncConsumerFailedException(e.getCause());
            }
            manager.commitBatch(f);
            return true;
        });
    }

    private void pushMessages(List<Message> messages) {
        Map<TopicPartition, List<Message>> partitionedMessages = new HashMap<>();
        messages.forEach(m -> partitionedMessages.computeIfAbsent(
                new TopicPartition(m.getTopic(), m.getPartition()), x -> new ArrayList<>()).add(m));
        for (List<Message> messageList : partitionedMessages.values()) {
            Future<List<Message>> future = executorService.submit(() -> {
                synchronized (sink) {
                    return sink.pushMessage(messageList);
                }
            });
            futures.add(future);
            manager.addOffsetToBatch(future, messageList.get(messageList.size() - 1));
        }
    }

    @Override
    public void close() throws IOException {
        if (consumer != null) {
            consumer.close();
        }
        sink.close();
    }
}
