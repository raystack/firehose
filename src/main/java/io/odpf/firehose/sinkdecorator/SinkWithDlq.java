package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.sinkdecorator.dlq.DlqWriter;

import java.io.IOException;
import java.util.List;

/**
 * This Sink pushes failed messages to kafka after retries are exhausted.
 */
public class SinkWithDlq extends SinkDecorator {

    public static final String DLQ_BATCH_KEY = "dlq-batch-key";
    private final DlqWriter writer;
    private Instrumentation instrumentation;

    public SinkWithDlq(Sink sink, DlqWriter writer, Instrumentation instrumentation) {
        super(sink);
        this.writer = writer;
        this.instrumentation = instrumentation;
    }

    public static SinkWithDlq withInstrumentationFactory(Sink sink, DlqWriter dlqWriter, StatsDReporter statsDReporter) {
        return new SinkWithDlq(sink, dlqWriter, new Instrumentation(statsDReporter, SinkWithDlq.class));
    }

    /**
     * Pushes all the failed messages to kafka as DLQ.
     *
     * @param message list of messages to push
     * @return list of failed messaged
     * @throws IOException
     * @throws DeserializerException
     */
    @Override
    public List<Message> pushMessage(List<Message> message) throws IOException, DeserializerException {
        List<Message> retryQueueMessages = super.pushMessage(message);

        List<Message> unsuccessfulDLQWrite;
        if (super.canManageOffsets()) {
            super.addOffsetToBatch(DLQ_BATCH_KEY, retryQueueMessages);
            unsuccessfulDLQWrite = writer.write(retryQueueMessages);
            super.setCommittable(DLQ_BATCH_KEY);
        } else {
            unsuccessfulDLQWrite = writer.write(retryQueueMessages);
        }

        instrumentation.logError("failed to write {} number messages to DLQ", unsuccessfulDLQWrite.size());
        return unsuccessfulDLQWrite;
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
