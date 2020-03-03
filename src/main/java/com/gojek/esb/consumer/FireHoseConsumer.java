package com.gojek.esb.consumer;

import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.filter.EsbFilterException;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.tracer.SinkTracer;
import com.gojek.esb.util.Clock;
import io.opentracing.Span;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static com.gojek.esb.metrics.Metrics.PARTITION_PROCESS_TIME;

@AllArgsConstructor
public class FireHoseConsumer implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FireHoseConsumer.class);

    private final EsbGenericConsumer consumer;
    private final Sink sink;
    private final StatsDReporter statsDReporter;
    private final Clock clock;
    private final SinkTracer tracer;

    public void processPartitions() throws IOException, DeserializerException, EsbFilterException {
        Instant beforeCall = clock.now();
        try {
            List<EsbMessage> messages = consumer.readMessages();
            List<Span> spans = tracer.startTrace(messages);

            if (!messages.isEmpty()) {
                sink.pushMessage(messages);
            }
            consumer.commit();
            LOGGER.info("Execution successful for {} records", messages.size());
            tracer.finishTrace(spans);
        } finally {
            statsDReporter.captureDurationSince(PARTITION_PROCESS_TIME, beforeCall);
        }
    }

    @Override
    public void close() throws IOException {
        if (consumer != null) {
            LOGGER.info("closing consumer");
            tracer.close();
            consumer.close();
        }
        sink.close();
        statsDReporter.close();
    }
}
