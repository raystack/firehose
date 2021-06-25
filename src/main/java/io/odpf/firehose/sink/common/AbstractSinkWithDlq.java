package io.odpf.firehose.sink.common;

import io.odpf.firehose.consumer.MessageWithError;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sinkdecorator.dlq.DlqWriter;

import java.io.IOException;
import java.util.List;

public abstract class AbstractSinkWithDlq extends AbstractSink {

    private final DlqWriter dlqWriter;

    public AbstractSinkWithDlq(Instrumentation instrumentation, String sinkType, DlqWriter dlqWriter) {
        super(instrumentation, sinkType);
        this.dlqWriter = dlqWriter;
    }

    public void sendToDLQ(List<MessageWithError> messages) throws IOException {
        List<MessageWithError> unProcessedMessage = dlqWriter.writeWithError(messages);
        getInstrumentation().logError("failed to write {} number of messages to DLQ: \n{}", unProcessedMessage.size(), unProcessedMessage);
    }
}
