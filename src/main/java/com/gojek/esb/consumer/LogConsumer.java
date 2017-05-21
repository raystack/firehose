package com.gojek.esb.consumer;

import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.exception.EsbFilterException;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.util.Clock;
import com.newrelic.api.agent.Trace;
import com.timgroup.statsd.StatsDClient;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

@AllArgsConstructor
public class LogConsumer {

    private static final Logger logger = LoggerFactory.getLogger(LogConsumer.class);

    private final EsbGenericConsumer consumer;

    public Sink getSink() {
        return sink;
    }

    private final Sink sink;
    private final StatsDClient statsDClient;

    private final Clock clock;

    @Trace(dispatcher = true)
    public void processPartitions() throws IOException, DeserializerException, EsbFilterException {
        Instant beforeCall = clock.now();
        String batchReceivedCounter = "messages.received";
        String batchSize = "messages.batch.size";

        try {
            List<EsbMessage> messages = consumer.readMessages();
            statsDClient.count(batchReceivedCounter, messages.size());
            statsDClient.gauge(batchSize, messages.size());

            if (!messages.isEmpty()) {

                sink.pushMessage((messages));
                logger.info("Execution successful for {} records", messages.size());
                consumer.commit();
            }
        } finally {
            String timeTakenKey = "messages.process_partitions_time";
            Instant afterCall = clock.now();
            statsDClient.recordExecutionTime(timeTakenKey, Duration.between(beforeCall, afterCall).toMillis());
        }
    }

    public void close() {
        if (consumer != null) {
            consumer.commit();
         ///   consumer.close();
        }
    }
}