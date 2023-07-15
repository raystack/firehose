package org.raystack.firehose.consumer;

import org.raystack.firehose.consumer.kafka.ConsumerAndOffsetManager;
import org.raystack.firehose.exception.FirehoseConsumerFailedException;
import org.raystack.firehose.message.Message;
import org.raystack.firehose.metrics.FirehoseInstrumentation;
import org.raystack.firehose.sink.SinkPool;
import org.raystack.firehose.filter.FilterException;
import org.raystack.firehose.filter.FilteredMessages;
import org.raystack.firehose.tracer.SinkTracer;
import io.opentracing.Span;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Future;

import static org.raystack.firehose.metrics.Metrics.SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS;

@AllArgsConstructor
public class FirehoseAsyncConsumer implements FirehoseConsumer {
    private final SinkPool sinkPool;
    private final SinkTracer tracer;
    private final ConsumerAndOffsetManager consumerAndOffsetManager;
    private final FirehoseFilter firehoseFilter;
    private final FirehoseInstrumentation firehoseInstrumentation;

    @Override
    public void process() {
        Instant beforeCall = Instant.now();
        try {
            List<Message> messages = consumerAndOffsetManager.readMessages();
            List<Span> spans = tracer.startTrace(messages);
            FilteredMessages filteredMessages = firehoseFilter.applyFilter(messages);
            if (filteredMessages.sizeOfInvalidMessages() > 0) {
                consumerAndOffsetManager.forceAddOffsetsAndSetCommittable(filteredMessages.getInvalidMessages());
            }
            if (filteredMessages.sizeOfValidMessages() > 0) {
                List<Message> validMessages = filteredMessages.getValidMessages();
                Future<List<Message>> scheduledTask = scheduleTask(validMessages);
                consumerAndOffsetManager.addOffsets(scheduledTask, validMessages);
            }
            sinkPool.fetchFinishedSinkTasks().forEach(consumerAndOffsetManager::setCommittable);
            consumerAndOffsetManager.commit();
            tracer.finishTrace(spans);
        } catch (FilterException e) {
            throw new FirehoseConsumerFailedException(e);
        } finally {
            firehoseInstrumentation.captureDurationSince(SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS, beforeCall);
        }
    }

    private Future<List<Message>> scheduleTask(List<Message> messages) {
        while (true) {
            Future<List<Message>> scheduledTask = sinkPool.submitTask(messages);
            if (scheduledTask == null) {
                firehoseInstrumentation.logInfo("The Queue is full");
                sinkPool.fetchFinishedSinkTasks().forEach(consumerAndOffsetManager::setCommittable);
            } else {
                firehoseInstrumentation.logInfo("Adding sink task");
                return scheduledTask;
            }
        }
    }

    @Override
    public void close() throws IOException {
        sinkPool.close();
        consumerAndOffsetManager.close();
        tracer.close();
        firehoseInstrumentation.close();
    }
}
