package com.gojek.esb.consumer;

import com.gojek.esb.client.GenericHTTPClient;
import com.gojek.esb.util.Clock;
import com.timgroup.statsd.StatsDClient;
import lombok.AllArgsConstructor;
import org.apache.http.HttpResponse;
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
    private final GenericHTTPClient genericHTTPClient;
    private final StatsDClient statsDClient;

    private final Clock clock;

    public void processPartitions() throws IOException {
        Instant beforeCall = clock.now();
        String batchReceivedCounter = "messages.received";
        String batchSize = "messages.batch.size";

        try {
            List<EsbMessage> messages = consumer.readMessages();
            statsDClient.count(batchReceivedCounter, messages.size());
            statsDClient.gauge(batchSize, messages.size());

            if (!messages.isEmpty()) {
                HttpResponse resp = genericHTTPClient.execute(messages);
                logger.info("Execution successful for {} records", messages.size());

                consumer.commitAsync();
            }
        } finally {
            String timeTakenKey = "messages.process_partitions_time";
            Instant afterCall = clock.now();
            statsDClient.recordExecutionTime(timeTakenKey, Duration.between(beforeCall, afterCall).toMillis());
        }
    }
}