package io.odpf.firehose.sinkdecorator.dlq;

import io.odpf.firehose.consumer.MessageWithError;
import io.odpf.firehose.metrics.Instrumentation;

import java.io.IOException;
import java.util.List;

public class LogDlqWriter extends ErrorWrapperDlqWriter {
    private final Instrumentation instrumentation;

    public LogDlqWriter(Instrumentation instrumentation) {
        this.instrumentation = instrumentation;
    }

    @Override
    public List<MessageWithError> writeWithError(List<MessageWithError> messages) throws IOException {
        for (MessageWithError message : messages) {
            String key = new String(message.getMessage().getLogKey());
            String value = new String(message.getMessage().getLogMessage());
            String error = message.getErrorType().toString();
            instrumentation.logInfo("key: {}\nvalue: {}\nerror: {}", key, value, error);
        }
        return null;
    }

}
