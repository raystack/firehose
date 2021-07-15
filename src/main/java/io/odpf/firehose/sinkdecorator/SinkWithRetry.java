package io.odpf.firehose.sinkdecorator;

import com.google.protobuf.DynamicMessage;
import io.odpf.firehose.config.AppConfig;
import io.odpf.firehose.config.DlqConfig;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.sink.log.KeyOrMessageParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static io.odpf.firehose.metrics.Metrics.RETRY_TOTAL;

/**
 * Pushes messages with configured retry.
 */
public class SinkWithRetry extends SinkDecorator {

    private final BackOffProvider backOffProvider;
    private final Instrumentation instrumentation;
    private final AppConfig appConfig;
    private final KeyOrMessageParser parser;
    private final ErrorMatcher retryErrorMatcher;

    public SinkWithRetry(Sink sink, BackOffProvider backOffProvider, Instrumentation instrumentation, AppConfig appConfig, KeyOrMessageParser parser, ErrorMatcher retryErrorMatcher) {
        super(sink);
        this.backOffProvider = backOffProvider;
        this.instrumentation = instrumentation;
        this.appConfig = appConfig;
        this.parser = parser;
        this.retryErrorMatcher = retryErrorMatcher;
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
        List<Message> failedMessages = super.pushMessage(messages);
        if (failedMessages.isEmpty()) {
            return failedMessages;
        }

        List<Message> retryMessages = new LinkedList<>();
        List<Message> skipMessages = new LinkedList<>();
        failedMessages.forEach(message -> {
            if (retryErrorMatcher.isMatch(message)) {
                retryMessages.add(message);
            } else {
                skipMessages.add(message);
            }
        });

        failedMessages = retry(retryMessages);
        if (!failedMessages.isEmpty() && appConfig.getFailOnMaxRetryAttempts()) {
            throw new IOException("exceeded maximum Sink retry attempts");
        }

        failedMessages.addAll(skipMessages);
        return failedMessages;
    }

    private List<Message> retry(List<Message> messages) throws IOException {
        List<Message> retryMessages = new LinkedList<>(messages);
        DlqConfig dlqConfig = (DlqConfig) appConfig;
        instrumentation.logWarn("Maximum retry attemps: {}", dlqConfig.getDlqAttemptsToTrigger());

        int attemptCount = 0;
        while ((attemptCount < dlqConfig.getDlqAttemptsToTrigger() && !retryMessages.isEmpty())
                || (dlqConfig.getDlqAttemptsToTrigger() == Integer.MAX_VALUE && !retryMessages.isEmpty())) {
            attemptCount++;
            instrumentation.incrementCounter(RETRY_TOTAL);
            instrumentation.logWarn("Retrying messages attempt count: {}, Number of messages: {}", attemptCount, messages.size());

            List<DynamicMessage> serializedBody = new ArrayList<>();
            for (Message message : retryMessages) {
                serializedBody.add(parser.parse(message));
            }

            instrumentation.logDebug("Retry failed messages: \n{}", serializedBody.toString());
            retryMessages = super.pushMessage(retryMessages);
            backOffProvider.backOff(attemptCount);
        }

        return retryMessages;
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
