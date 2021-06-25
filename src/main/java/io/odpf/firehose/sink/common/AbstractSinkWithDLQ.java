package io.odpf.firehose.sink.common;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sinkdecorator.dlq.DlqWriter;

import java.io.IOException;
import java.util.List;

public abstract class AbstractSinkWithDLQ extends AbstractSink {

    private final DlqWriter dlqWriter;

    public AbstractSinkWithDLQ(Instrumentation instrumentation, String sinkType, DlqWriter dlqWriter) {
        super(instrumentation, sinkType);
        this.dlqWriter = dlqWriter;
    }

    public void sendToDLQ(List<Message> messages) throws IOException {
        List<Message> unProcessedMessage = dlqWriter.write(messages);
        getInstrumentation().logError("failed to write {} number of messages to DLQ: \n{}", unProcessedMessage.size(), unProcessedMessage);
    }
}
