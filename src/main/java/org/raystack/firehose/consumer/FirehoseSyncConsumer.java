package org.raystack.firehose.consumer;

import org.raystack.firehose.consumer.kafka.ConsumerAndOffsetManager;
import org.raystack.firehose.exception.FirehoseConsumerFailedException;
import org.raystack.firehose.message.Message;
import org.raystack.firehose.filter.FilterException;
import org.raystack.firehose.filter.FilteredMessages;
import org.raystack.firehose.metrics.FirehoseInstrumentation;
import org.raystack.firehose.sink.Sink;
import org.raystack.firehose.tracer.SinkTracer;
import io.opentracing.Span;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.raystack.firehose.metrics.Metrics.SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS;

/**
 * Firehose consumer reads messages from Generic consumer and pushes messages to the configured sink.
 */
@AllArgsConstructor
public class FirehoseSyncConsumer implements FirehoseConsumer {

    private final Sink sink;
    private final SinkTracer tracer;
    private final ConsumerAndOffsetManager consumerAndOffsetManager;
    private final FirehoseFilter firehoseFilter;
    private final FirehoseInstrumentation firehoseInstrumentation;

    @Override
    public void process() throws IOException {
        Instant beforeCall = Instant.now();
        try {
            List<Message> messages = consumerAndOffsetManager.readMessages();
            List<Span> spans = tracer.startTrace(messages);
            FilteredMessages filteredMessages = firehoseFilter.applyFilter(messages);
            if (filteredMessages.sizeOfInvalidMessages() > 0) {
                consumerAndOffsetManager.forceAddOffsetsAndSetCommittable(filteredMessages.getInvalidMessages());
            }
            if (filteredMessages.sizeOfValidMessages() > 0) {
                sink.pushMessage(filteredMessages.getValidMessages());
                consumerAndOffsetManager.addOffsetsAndSetCommittable(filteredMessages.getValidMessages());
            }
            consumerAndOffsetManager.commit();
            firehoseInstrumentation.logInfo("Processed {} records in consumer", messages.size());
            tracer.finishTrace(spans);
        } catch (FilterException e) {
            throw new FirehoseConsumerFailedException(e);
        } finally {
            firehoseInstrumentation.captureDurationSince(SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS, beforeCall);
        }
    }

    @Override
    public void close() throws IOException {
        sink.close();
        tracer.close();
        consumerAndOffsetManager.close();
        firehoseInstrumentation.close();
    }
}
