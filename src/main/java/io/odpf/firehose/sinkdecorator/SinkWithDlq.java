package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.sinkdecorator.dlq.DlqWriter;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

/**
 * This Sink pushes failed messages to kafka after retries are exhausted.
 */
public class SinkWithDlq extends SinkDecorator {

    public static final String DLQ_BATCH_KEY = "dlq-batch-key";
    private final DlqWriter writer;
    private final BackOffProvider backOffProvider;
    private final int maxRetryAttempts;
    private final boolean isFailOnMaxRetryAttemptsExceeded;

    private Instrumentation instrumentation;

    public SinkWithDlq(Sink sink, DlqWriter writer, BackOffProvider backOffProvider, int maxRetryAttempts, boolean isFailOnMaxRetryAttemptsExceeded, Instrumentation instrumentation) {
        super(sink);
        this.writer = writer;
        this.backOffProvider = backOffProvider;
        this.maxRetryAttempts = maxRetryAttempts;
        this.isFailOnMaxRetryAttemptsExceeded = isFailOnMaxRetryAttemptsExceeded;
        this.instrumentation = instrumentation;
    }

    public static SinkWithDlq withInstrumentationFactory(Sink sink, DlqWriter dlqWriter, BackOffProvider backOffProvider, int maxRetryAttempts, boolean isFailOnMaxRetryAttemptsExceeded, StatsDReporter statsDReporter) {
        return new SinkWithDlq(sink, dlqWriter, backOffProvider, maxRetryAttempts, isFailOnMaxRetryAttemptsExceeded, new Instrumentation(statsDReporter, SinkWithDlq.class));
    }

    /**
     * Pushes all the failed messages to kafka as DLQ.
     *
     * @param messages list of messages to push
     * @return list of failed messaged
     * @throws IOException
     * @throws DeserializerException
     */
    @Override
    public List<Message> pushMessage(List<Message> messages) throws IOException, DeserializerException {
        List<Message> retryQueueMessages = super.pushMessage(messages);
        if (super.canManageOffsets()) {
            super.addOffsets(DLQ_BATCH_KEY, retryQueueMessages);
            retryQueueMessages = pushToWriter(retryQueueMessages);
            super.setCommittable(DLQ_BATCH_KEY);
        } else {
            retryQueueMessages = pushToWriter(retryQueueMessages);
        }
        return retryQueueMessages;
    }

    private List<Message> pushToWriter(List<Message> retryQueueMessages) throws IOException {
        int attemptCount = 0;
        while (attemptCount < maxRetryAttempts && !retryQueueMessages.isEmpty()) {
            instrumentation.captureRetryAttempts();
            int remainingRetryMessagesCount = retryQueueMessages.size();

            retryQueueMessages = writer.write(retryQueueMessages);

            retryQueueMessages.forEach(message -> instrumentation.incrementMessageFailCount(message, message.getErrorInfo().getException()));
            int processedMessagesCount = remainingRetryMessagesCount - retryQueueMessages.size();
            IntStream.range(0, processedMessagesCount).forEach(value -> instrumentation.incrementMessageSucceedCount());

            backOffProvider.backOff(attemptCount++);
        }

        if (!retryQueueMessages.isEmpty() && isFailOnMaxRetryAttemptsExceeded) {
            throw new IOException("exhausted maximum number of allowed retry attempts to write messages to DLQ");
        }

        return retryQueueMessages;
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
