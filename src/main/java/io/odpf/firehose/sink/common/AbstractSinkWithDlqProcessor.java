package io.odpf.firehose.sink.common;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.MessageWithError;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sinkdecorator.dlq.DlqWriter;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public abstract class AbstractSinkWithDlqProcessor extends AbstractSink {

    private final DlqWriter dlqWriter;

    public AbstractSinkWithDlqProcessor(Instrumentation instrumentation, String sinkType, DlqWriter dlqWriter) {
        super(instrumentation, sinkType);
        this.dlqWriter = dlqWriter;
    }

    public List<MessageWithError> processDlq(List<MessageWithError> messages) throws IOException {
        return dlqWriter.writeWithError(messages);
    }

    @Override
    protected List<Message> execute() throws Exception {
        return new LinkedList<>();
    }
}
