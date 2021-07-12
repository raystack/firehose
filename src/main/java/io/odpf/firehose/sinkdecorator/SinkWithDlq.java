package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.config.DlqConfig;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.sinkdecorator.dlq.DlqWriter;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
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

    private final Instrumentation instrumentation;

    public SinkWithDlq(Sink sink, DlqWriter writer, BackOffProvider backOffProvider, DlqConfig dlqConfig, Instrumentation instrumentation) {
        super(sink);
        this.writer = writer;
        this.backOffProvider = backOffProvider;
        this.instrumentation = instrumentation;
        this.dlqConfig = dlqConfig;
    }

    public static SinkWithDlq withInstrumentationFactory(Sink sink, DlqWriter dlqWriter, BackOffProvider backOffProvider, DlqConfig dlqConfig, StatsDReporter statsDReporter) {
        return new SinkWithDlq(sink, dlqWriter, backOffProvider, dlqConfig, new Instrumentation(statsDReporter, SinkWithDlq.class));
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
        List<Message> dlqMessages = super.pushMessage(inputMessages);
        if (dlqMessages.isEmpty()) {
            return dlqMessages;
        }

        List<Message> dlqWriteFailedMessages = pushToWriter(dlqMessages);
        instrumentation.logInfo("DLQ processed messages: {}", dlqMessages.size() - dlqWriteFailedMessages.size());

        if (!dlqWriteFailedMessages.isEmpty()) {
            if (this.dlqConfig.getDlqFailOnMaxRetryAttemptsExceeded()) {
                throw new IOException("exhausted maximum number of allowed retry attempts to write messages to DLQ");
            }
            instrumentation.logInfo("failed to be processed by DLQ messages: {}", dlqWriteFailedMessages.size());
        }

        if (super.canManageOffsets()) {
            super.addOffsets(DLQ_BATCH_KEY, dlqMessages);
            super.setCommittable(DLQ_BATCH_KEY);
        }

        return dlqWriteFailedMessages;
    }

    private List<Message> pushToWriter(List<Message> messages) throws IOException {
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

        return retryQueueMessages;
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
