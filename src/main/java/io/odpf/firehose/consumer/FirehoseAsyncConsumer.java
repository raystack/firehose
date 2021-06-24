package io.odpf.firehose.consumer;

import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.tracer.SinkTracer;
import io.odpf.firehose.util.Clock;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import static io.odpf.firehose.metrics.Metrics.SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS;

@AllArgsConstructor
public class FirehoseAsyncConsumer implements KafkaConsumer {
    private static final int SLEEP = 10;
    private final Sink sink;
    private final Clock clock;
    private final SinkTracer tracer;
    private final ConsumerAndOffsetManager consumerAndOffsetManager;
    private final Instrumentation instrumentation;
    private final ExecutorService executorService;
    private final Set<Future<?>> futures = new HashSet<>();

    @Override
    public void process() throws FilterException {
        Instant beforeCall = clock.now();
        try {
            List<Message> messages = consumerAndOffsetManager.readMessagesFromKafka();
            if (!messages.isEmpty()) {
                pushMessages(messages);
            }
            checkFinishedSinkTasks();
            consumerAndOffsetManager.commit();
        } finally {
            instrumentation.captureDurationSince(SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS, beforeCall);
        }
    }

    private void checkFinishedSinkTasks() {
        futures.removeIf(f -> {
            if (!f.isDone()) {
                return false;
            }
            try {
                f.get();
                instrumentation.logInfo("Finishing Sink Task");
            } catch (InterruptedException e) {
                throw new AsyncConsumerFailedException(e);
            } catch (ExecutionException e) {
                throw new AsyncConsumerFailedException(e.getCause());
            }
            consumerAndOffsetManager.setCommittable(f);
            return true;
        });
    }

    private void pushMessages(List<Message> messages) {
        Future<List<Message>> future = submit(new SinkTask(sink, messages));
        futures.add(future);
        instrumentation.logInfo("Adding Sink Task");
        consumerAndOffsetManager.addOffsets(future, messages);
    }

    private Future<List<Message>> submit(Callable<List<Message>> task) {
        while (true) {
            try {
                return executorService.submit(task);
            } catch (RejectedExecutionException e) {
                instrumentation.logInfo("Waiting for threads to be scheduled. Queue is Full.");
            }
        }
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
        consumerAndOffsetManager.close();
        sink.close();
        tracer.close();
        instrumentation.close();
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
