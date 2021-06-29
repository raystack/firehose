package io.odpf.firehose.consumer;

import io.odpf.firehose.sink.Sink;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@AllArgsConstructor
public class SinkPool implements AutoCloseable {
    private final Set<SinkFuture> sinkFutures = new HashSet<>();
    private final BlockingQueue<Sink> workerSinks;
    private final ExecutorService executorService;
    private final long defaultPollTimeOutMillis;

    public Set<Future<List<Message>>> fetchFinishedSinkTasks() {
        Set<SinkFuture> finished = sinkFutures.stream().filter(x -> {
            if (x.getFuture().isDone()) {
                try {
                    x.getFuture().get();
                    workerSinks.put(x.getSink());
                } catch (InterruptedException e) {
                    throw new AsyncConsumerFailedException(e);
                } catch (ExecutionException e) {
                    throw new AsyncConsumerFailedException(e.getCause());
                }
                return true;
            } else {
                return false;
            }
        }).collect(Collectors.toSet());
        sinkFutures.removeIf(finished::contains);
        return finished.stream().map(SinkFuture::getFuture).collect(Collectors.toSet());
    }

    public Future<List<Message>> submitTask(List<Message> messages) {
        try {
            Sink s = workerSinks.poll(defaultPollTimeOutMillis, TimeUnit.MILLISECONDS);
            if (s == null) {
                return null;
            }
            Future<List<Message>> future = executorService.submit(new SinkTask(s, messages));
            SinkFuture sinkFuture = new SinkFuture(future, s);
            sinkFutures.add(sinkFuture);
            return future;
        } catch (InterruptedException e) {
            return null;
        }
    }

    @Override
    public void close() {

    }

    @Data
    @AllArgsConstructor
    protected static class SinkFuture {
        private Future<List<Message>> future;
        private Sink sink;
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

