package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.config.DlqConfig;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.error.ErrorHandler;
import io.odpf.firehose.error.ErrorScope;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.sinkdecorator.dlq.DlqWriter;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

/**
 * This Sink pushes failed messages to kafka after retries are exhausted.
 */
public class SinkWithDlq extends SinkDecorator {

    public static final String DLQ_BATCH_KEY = "dlq-batch-key";
    private final DlqWriter writer;
    private final BackOffProvider backOffProvider;
    private final DlqConfig dlqConfig;
    private final ErrorHandler errorHandler;

    private final Instrumentation instrumentation;

    public SinkWithDlq(Sink sink, DlqWriter writer, BackOffProvider backOffProvider, DlqConfig dlqConfig, ErrorHandler errorHandler, Instrumentation instrumentation) {
        super(sink);
        this.writer = writer;
        this.backOffProvider = backOffProvider;
        this.errorHandler = errorHandler;
        this.instrumentation = instrumentation;
        this.dlqConfig = dlqConfig;
    }

    public static SinkWithDlq withInstrumentationFactory(Sink sink, DlqWriter dlqWriter, BackOffProvider backOffProvider, DlqConfig dlqConfig, ErrorHandler errorHandler, StatsDReporter statsDReporter) {
        return new SinkWithDlq(sink, dlqWriter, backOffProvider, dlqConfig, errorHandler, new Instrumentation(statsDReporter, SinkWithDlq.class));
    }

    /**
     * Pushes all the failed messages to kafka as DLQ.
     *
     * @param inputMessages list of messages to push
     * @return list of failed messaged
     * @throws IOException
     * @throws DeserializerException
     */
    @Override
    public List<Message> pushMessage(List<Message> inputMessages) throws IOException, DeserializerException {
        List<Message> messages = super.pushMessage(inputMessages);
        if (messages.isEmpty()) {
            return messages;
        }
        Map<Boolean, List<Message>> splitLists = errorHandler.split(messages, ErrorScope.DLQ);

        List<Message> returnedMessages = doDLQ(splitLists.get(Boolean.TRUE));

        if (!returnedMessages.isEmpty() && dlqConfig.getDlqFailOnMaxRetryAttemptsExceeded()) {
            throw new IOException("exhausted maximum number of allowed retry attempts to write messages to DLQ");
        }
        if (super.canManageOffsets()) {
            super.addOffsets(DLQ_BATCH_KEY, messages);
            super.setCommittable(DLQ_BATCH_KEY);
        }
        returnedMessages.addAll(splitLists.get(Boolean.FALSE));
        return returnedMessages;
    }

    private List<Message> doDLQ(List<Message> messages) throws IOException {
        List<Message> retryQueueMessages = new LinkedList<>(messages);
        int attemptCount = 0;
        while (attemptCount < this.dlqConfig.getDlqMaxRetryAttempts() && !retryQueueMessages.isEmpty()) {
            instrumentation.captureRetryAttempts();
            int remainingRetryMessagesCount = retryQueueMessages.size();

            retryQueueMessages = writer.write(retryQueueMessages);

            retryQueueMessages.forEach(message -> Optional.ofNullable(message.getErrorInfo())
                    .flatMap(errorInfo -> Optional.ofNullable(errorInfo.getException()))
                    .ifPresent(e -> instrumentation.incrementMessageFailCount(message, e)));

            int processedMessagesCount = remainingRetryMessagesCount - retryQueueMessages.size();
            IntStream.range(0, processedMessagesCount).forEach(value -> instrumentation.incrementMessageSucceedCount());

            backOffProvider.backOff(attemptCount++);
        }
        if (!retryQueueMessages.isEmpty()) {
            instrumentation.logInfo("failed to be processed by DLQ messages: {}", retryQueueMessages.size());
        }
        return retryQueueMessages;
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
