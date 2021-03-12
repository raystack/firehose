package com.gojek.esb.sinkdecorator;

import com.gojek.esb.consumer.Message;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.sink.log.KeyOrMessageParser;
import com.google.protobuf.DynamicMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.gojek.esb.metrics.Metrics.RETRY_COUNT;

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

    @Override
    public List<Message> pushMessage(List<Message> esbMessage) throws IOException, DeserializerException {
        int attemptCount = 0;
        List<Message> failedMessages;
        failedMessages = super.pushMessage(esbMessage);
        if (failedMessages.isEmpty()) {
            return failedMessages;
        }
        instrumentation.logWarn("Maximum retry attemps: {}", maxRetryAttempts);

        while ((attemptCount < maxRetryAttempts && !failedMessages.isEmpty())
                || (maxRetryAttempts == Integer.MAX_VALUE && !failedMessages.isEmpty())) {
            attemptCount++;
            instrumentation.incrementCounter(RETRY_COUNT);
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
