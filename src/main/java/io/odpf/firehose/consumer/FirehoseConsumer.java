package io.odpf.firehose.consumer;

import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.tracer.SinkTracer;
import io.odpf.firehose.util.Clock;
import io.opentracing.Span;
import lombok.AllArgsConstructor;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static io.odpf.firehose.metrics.Metrics.SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS;

/**
 * Firehose consumer reads messages from Generic consumer and pushes messages to the configured sink.
 */
@AllArgsConstructor
public class FirehoseConsumer implements Closeable {

    private final GenericConsumer consumer;
    private final Sink sink;
    private final Clock clock;
    private final SinkTracer tracer;
    private final Instrumentation instrumentation;

    public void processPartitions() throws IOException, DeserializerException, FilterException {
        Instant beforeCall = clock.now();
        try {
            List<Message> messages = consumer.readMessages();
            List<Span> spans = tracer.startTrace(messages);

            if (!messages.isEmpty()) {
                sink.pushMessage(messages);
            }
            consumer.commit();
            instrumentation.logInfo("Execution successful for {} records", messages.size());
            tracer.finishTrace(spans);
        } finally {
            instrumentation.captureDurationSince(SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS, beforeCall);
        }
    }

    @Override
    public void close() throws IOException {
        if (consumer != null) {
            instrumentation.logInfo("closing consumer");
            tracer.close();
            consumer.close();
        }
        sink.close();
        instrumentation.close();
    }
}
