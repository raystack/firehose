package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.sink.log.KeyOrMessageParser;
import com.google.protobuf.DynamicMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.odpf.firehose.metrics.Metrics.RETRY_TOTAL;

/**
 * Pushes messages with configured retry.
 */
public class SinkWithRetry extends SinkDecorator {

    private final BackOffProvider backOffProvider;
    private Instrumentation instrumentation;
    private int maxRetryAttempts;
    private KeyOrMessageParser parser;

    public SinkWithRetry(Sink sink, BackOffProvider backOffProvider, Instrumentation instrumentation,
                         int maxRetryAttempts, KeyOrMessageParser parser) {
        super(sink);
        this.backOffProvider = backOffProvider;
        this.instrumentation = instrumentation;
        this.maxRetryAttempts = maxRetryAttempts;
        this.parser = parser;
    }

    public SinkWithRetry(Sink sink, BackOffProvider backOffProvider, Instrumentation instrumentation,
                         KeyOrMessageParser parser) {
        super(sink);
        this.maxRetryAttempts = Integer.MAX_VALUE;
        this.backOffProvider = backOffProvider;
        this.instrumentation = instrumentation;
        this.parser = parser;
    }

    /**
     * Pushes messages with retry.
     *
     * @param messages list of messages
     * @return the remaining failed messages
     * @throws IOException           the io exception
     * @throws DeserializerException the deserializer exception
     */
    @Override
    public List<Message> pushMessage(List<Message> messages) throws IOException, DeserializerException {
        int attemptCount = 0;
        List<Message> failedMessages;
        failedMessages = super.pushMessage(messages);
        if (failedMessages.isEmpty()) {
            return failedMessages;
        }
        instrumentation.logWarn("Maximum retry attemps: {}", maxRetryAttempts);

        while ((attemptCount < maxRetryAttempts && !failedMessages.isEmpty())
                || (maxRetryAttempts == Integer.MAX_VALUE && !failedMessages.isEmpty())) {
            attemptCount++;
            instrumentation.incrementCounter(RETRY_TOTAL);
            instrumentation.logWarn("Retrying messages attempt count: {}, Number of messages: {}", attemptCount, failedMessages.size());

            List<DynamicMessage> serializedBody = new ArrayList<>();
            for (Message message : failedMessages) {
                serializedBody.add(parser.parse(message));
            }

            instrumentation.logDebug("Retry failed messages: \n{}", serializedBody.toString());
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
