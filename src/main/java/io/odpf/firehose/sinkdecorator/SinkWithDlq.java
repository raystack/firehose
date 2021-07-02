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

    private Instrumentation instrumentation;

    public SinkWithDlq(Sink sink, DlqWriter writer, BackOffProvider backOffProvider, Instrumentation instrumentation) {
        super(sink);
        this.writer = writer;
        this.backOffProvider = backOffProvider;
        this.instrumentation = instrumentation;
    }

    public static SinkWithDlq withInstrumentationFactory(Sink sink, DlqWriter dlqWriter, BackOffProvider backOffProvider, StatsDReporter statsDReporter) {
        return new SinkWithDlq(sink, dlqWriter, backOffProvider, new Instrumentation(statsDReporter, SinkWithDlq.class));
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
        super.addOffsetToBatch(DLQ_BATCH_KEY, messages);
        List<Message> retryMessages = push(messages);
        super.setCommittable(DLQ_BATCH_KEY);
        return retryMessages;
    }

    private List<Message> push(List<Message> messages) throws IOException {
        List<Message> retryQueueMessages = super.pushMessage(messages);

        int attemptCount = 0;
        while (!retryQueueMessages.isEmpty()) {
            instrumentation.captureRetryAttempts();
            int remainingRetryMessagesCount = retryQueueMessages.size();

            retryQueueMessages = writer.write(retryQueueMessages);

            retryQueueMessages.forEach(message -> instrumentation.incrementMessageFailCount(message, message.getErrorInfo().getException()));
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
