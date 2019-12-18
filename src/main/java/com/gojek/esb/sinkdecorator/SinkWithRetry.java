package com.gojek.esb.sinkdecorator;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.Sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static com.gojek.esb.metrics.Metrics.REQUEST_RETRY;

public class SinkWithRetry extends SinkDecorator {

    private final BackOffProvider backOffProvider;
    private StatsDReporter statsDReporter;
    private int maxRetryAttempts;
    private static final Logger LOGGER = LoggerFactory.getLogger(SinkWithRetry.class);

    public SinkWithRetry(Sink sink, BackOffProvider backOffProvider, StatsDReporter statsDReporter,
            int maxRetryAttempts) {
        super(sink);
        this.backOffProvider = backOffProvider;
        this.statsDReporter = statsDReporter;
        this.maxRetryAttempts = maxRetryAttempts;
    }

    public SinkWithRetry(Sink sink, BackOffProvider backOffProvider, StatsDReporter statsDReporter) {
        super(sink);
        this.maxRetryAttempts = Integer.MAX_VALUE;
        this.backOffProvider = backOffProvider;
        this.statsDReporter = statsDReporter;
    }

    @Override
    public List<EsbMessage> pushMessage(List<EsbMessage> esbMessage) throws IOException, DeserializerException {
        int attemptCount = 0;
        List<EsbMessage> failedMessages;
        failedMessages = super.pushMessage(esbMessage);
        if (failedMessages.isEmpty()) {
            return failedMessages;
        }

        while ((attemptCount < maxRetryAttempts && !failedMessages.isEmpty())
                || (maxRetryAttempts == Integer.MAX_VALUE && !failedMessages.isEmpty())) {
            attemptCount++;
            this.statsDReporter.increment(REQUEST_RETRY);
            LOGGER.info("Retrying messages attempt count: {}", attemptCount);
            failedMessages = super.pushMessage(failedMessages);
            backOffProvider.backOff(attemptCount);
        }
        return failedMessages;
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
