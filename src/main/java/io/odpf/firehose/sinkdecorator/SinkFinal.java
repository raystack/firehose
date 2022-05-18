package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.message.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.firehose.metrics.Metrics;
import io.odpf.firehose.sink.Sink;

import java.io.IOException;
import java.util.List;

public class SinkFinal extends SinkDecorator {
    private final FirehoseInstrumentation firehoseInstrumentation;

    /**
     * Instantiates a new Sink decorator.
     *
     * @param sink wrapped sink object
     */

    public SinkFinal(Sink sink, FirehoseInstrumentation firehoseInstrumentation) {
        super(sink);
        this.firehoseInstrumentation = firehoseInstrumentation;
    }

    @Override
    public List<Message> pushMessage(List<Message> inputMessages) throws IOException, DeserializerException {
        List<Message> failedMessages = super.pushMessage(inputMessages);
        if (failedMessages.size() > 0) {
            firehoseInstrumentation.logInfo("Ignoring messages {}", failedMessages.size());
            firehoseInstrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.IGNORED, failedMessages.size());
        }
        return failedMessages;
    }
}
