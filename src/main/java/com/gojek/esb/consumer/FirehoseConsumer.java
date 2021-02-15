package com.gojek.esb.consumer;

import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.filter.FilterException;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.tracer.SinkTracer;
import com.gojek.esb.util.Clock;
import io.opentracing.Span;
import lombok.AllArgsConstructor;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static com.gojek.esb.metrics.Metrics.PARTITION_PROCESS_TIME;

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
            instrumentation.captureDurationSince(PARTITION_PROCESS_TIME, beforeCall);
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
