package com.gojek.esb.consumer;

import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.filter.EsbFilterException;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.util.Clock;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static com.gojek.esb.metrics.Metrics.MESSAGE_RECEIVED;
import static com.gojek.esb.metrics.Metrics.PARTITION_PROCESS_TIME;

@AllArgsConstructor
public class FireHoseConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(FireHoseConsumer.class);

    private final EsbGenericConsumer consumer;

    private final Sink sink;
    private final StatsDReporter statsDReporter;

    private final Clock clock;

    public void processPartitions() throws IOException, DeserializerException, EsbFilterException {
        Instant beforeCall = clock.now();

        try {
            List<EsbMessage> messages = consumer.readMessages();
            statsDReporter.captureCount(MESSAGE_RECEIVED, messages.size());
            if (!messages.isEmpty()) {
                sink.pushMessage(messages);
                LOGGER.info("Execution successful for {} records", messages.size());
            }

            consumer.commit();
        } finally {
            statsDReporter.captureDurationSince(PARTITION_PROCESS_TIME, beforeCall);
        }
    }

    public void close() {
        if (consumer != null) {
            LOGGER.info("closing consumer");
            consumer.close();
        }
    }
}
