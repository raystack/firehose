package io.odpf.firehose.consumer;

import io.odpf.firehose.consumer.common.FirehoseConsumer;
import io.odpf.firehose.type.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.filter.FilteredMessages;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.tracer.SinkTracer;
import io.opentracing.Span;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static io.odpf.firehose.metrics.Metrics.SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS;

/**
 * Firehose consumer reads messages from Generic consumer and pushes messages to the configured sink.
 */
@AllArgsConstructor
public class FirehoseSyncConsumer implements FirehoseConsumer {

    private final Sink sink;
    private final SinkTracer tracer;
    private final ConsumerAndOffsetManager consumerAndOffsetManager;
    private final FirehoseFilter firehoseFilter;
    private final Instrumentation instrumentation;

    @Override
    public void process() throws IOException, DeserializerException, FilterException {
        Instant beforeCall = Instant.now();
        try {
            List<Message> messages = consumerAndOffsetManager.readMessagesFromKafka();
            List<Span> spans = tracer.startTrace(messages);
            FilteredMessages filteredMessages = firehoseFilter.applyFilter(messages);
            consumerAndOffsetManager.addOffsetsAndSetCommittable(filteredMessages.getInvalidMessages());
            if (filteredMessages.sizeOfValidMessages() > 0) {
                sink.pushMessage(filteredMessages.getValidMessages());
                consumerAndOffsetManager.addOffsetsAndSetCommittable(filteredMessages.getValidMessages());
            }
            consumerAndOffsetManager.commit();
            instrumentation.logInfo("Processed {} records in consumer", messages.size());
            tracer.finishTrace(spans);
        } finally {
            instrumentation.captureDurationSince(SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS, beforeCall);
        }
    }

    @Override
    public void close() throws IOException {
        tracer.close();
        consumerAndOffsetManager.close();
        instrumentation.close();
        sink.close();
    }
}
