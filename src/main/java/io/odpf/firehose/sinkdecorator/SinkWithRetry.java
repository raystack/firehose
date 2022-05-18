package io.odpf.firehose.sinkdecorator;

import com.google.protobuf.DynamicMessage;
import io.odpf.firehose.config.AppConfig;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.error.ErrorHandler;
import io.odpf.firehose.error.ErrorScope;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.firehose.metrics.Metrics;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.sink.common.KeyOrMessageParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static io.odpf.firehose.metrics.Metrics.RETRY_MESSAGES_TOTAL;
import static io.odpf.firehose.metrics.Metrics.RETRY_ATTEMPTS_TOTAL;

/**
 * Pushes messages with configured retry.
 */
public class SinkWithRetry extends SinkDecorator {

    private final BackOffProvider backOffProvider;
    private final FirehoseInstrumentation firehoseInstrumentation;
    private final AppConfig appConfig;
    private final KeyOrMessageParser parser;
    private final ErrorHandler errorHandler;

    public SinkWithRetry(Sink sink, BackOffProvider backOffProvider, FirehoseInstrumentation firehoseInstrumentation, AppConfig appConfig, KeyOrMessageParser parser, ErrorHandler errorHandler) {
        super(sink);
        this.backOffProvider = backOffProvider;
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.appConfig = appConfig;
        this.parser = parser;
        this.errorHandler = errorHandler;
    }

    /**
     * Pushes messages with retry.
     *
     * @param inputMessages list of messages
     * @return the remaining failed messages
     * @throws IOException           the io exception
     * @throws DeserializerException the deserializer exception
     */
    @Override
    public List<Message> pushMessage(List<Message> inputMessages) throws IOException, DeserializerException {
        List<Message> failedMessages = super.pushMessage(inputMessages);
        if (failedMessages.isEmpty()) {
            return failedMessages;
        }
        Map<Boolean, List<Message>> splitLists = errorHandler.split(failedMessages, ErrorScope.RETRY);
        List<Message> messagesAfterRetry = doRetry(splitLists.get(Boolean.TRUE));
        if (!messagesAfterRetry.isEmpty() && appConfig.getRetryFailAfterMaxAttemptsEnable()) {
            throw new IOException("exceeded maximum Sink retry attempts");
        }
        messagesAfterRetry.addAll(splitLists.get(Boolean.FALSE));
        return messagesAfterRetry;
    }

    private void logDebug(List<Message> messageList) throws IOException {
        if (firehoseInstrumentation.isDebugEnabled()) {
            List<DynamicMessage> serializedBody = new ArrayList<>();
            for (Message message : messageList) {
                serializedBody.add(parser.parse(message));
            }
            firehoseInstrumentation.logDebug("Retry failed messages: \n{}", serializedBody.toString());
        }
    }

    private void backOff(List<Message> messageList, int attemptCount) {
        if (messageList.isEmpty()) {
            return;
        }
        backOffProvider.backOff(attemptCount);
    }

    private List<Message> doRetry(List<Message> messages) throws IOException {
        List<Message> retryMessages = new LinkedList<>(messages);
        firehoseInstrumentation.logInfo("Maximum retry attempts: {}", appConfig.getRetryMaxAttempts());
        retryMessages.forEach(m -> {
            m.setDefaultErrorIfNotPresent();
            firehoseInstrumentation.captureMessageMetrics(RETRY_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, m.getErrorInfo().getErrorType(), 1);
        });

        int attemptCount = 1;
        while ((attemptCount <= appConfig.getRetryMaxAttempts() && !retryMessages.isEmpty())
                || (appConfig.getRetryMaxAttempts() == Integer.MAX_VALUE && !retryMessages.isEmpty())) {
            firehoseInstrumentation.incrementCounter(RETRY_ATTEMPTS_TOTAL);
            firehoseInstrumentation.logInfo("Retrying messages attempt count: {}, Number of messages: {}", attemptCount, messages.size());
            logDebug(retryMessages);
            retryMessages = super.pushMessage(retryMessages);
            backOff(retryMessages, attemptCount);
            attemptCount++;
        }
        firehoseInstrumentation.captureMessageMetrics(RETRY_MESSAGES_TOTAL, Metrics.MessageType.SUCCESS, messages.size() - retryMessages.size());
        retryMessages.forEach(m -> firehoseInstrumentation.captureMessageMetrics(RETRY_MESSAGES_TOTAL, Metrics.MessageType.FAILURE, m.getErrorInfo().getErrorType(), 1));
        return retryMessages;
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
