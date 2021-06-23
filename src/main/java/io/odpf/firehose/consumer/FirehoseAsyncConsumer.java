package io.odpf.firehose.consumer;

import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.sink.Sink;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

@AllArgsConstructor
public class FirehoseAsyncConsumer implements KafkaConsumer {
    private final Sink sink;
    private final ExecutorService executorService;
    private final Set<Future<?>> futures = new HashSet<>();
    private final ConsumerOffsetManager consumerOffsetManager;

    @Override
    public void process() throws FilterException {
        List<Message> messages = consumerOffsetManager.readMessagesFromKafka();
        if (!messages.isEmpty()) {
            pushMessages(messages);
        }
        checkFinishedSinkTasks();
        consumerOffsetManager.commit();
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
            consumerOffsetManager.setCommittable(f);
            return true;
        });
    }

    private void pushMessages(List<Message> messages) {
        Map<TopicPartition, List<Message>> partitionedMessages = new HashMap<>();
        messages.forEach(m -> partitionedMessages.computeIfAbsent(
                new TopicPartition(m.getTopic(), m.getPartition()), x -> new ArrayList<>()).add(m));
        for (List<Message> messagesPerPartition : partitionedMessages.values()) {
            Future<List<Message>> future = executorService.submit(new SinkTask(sink, messagesPerPartition));
            futures.add(future);
            consumerOffsetManager.addPartitionedOffsets(future, messagesPerPartition);
        }
    }

    @Override
    public void close() throws IOException {
        consumerOffsetManager.close();
        sink.close();
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    protected static class SinkTask implements Callable<List<Message>> {
        private final Sink sink;
        private final List<Message> messages;

        @Override
        public List<Message> call() throws Exception {
            return sink.pushMessage(messages);
        }
    }
}
