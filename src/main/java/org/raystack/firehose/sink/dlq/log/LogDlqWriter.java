package org.raystack.firehose.sink.dlq.log;

import org.raystack.firehose.message.Message;
import org.raystack.firehose.metrics.FirehoseInstrumentation;
import org.raystack.depot.error.ErrorInfo;
import org.raystack.firehose.sink.dlq.DlqWriter;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class LogDlqWriter implements DlqWriter {
    private final FirehoseInstrumentation firehoseInstrumentation;

    public LogDlqWriter(FirehoseInstrumentation firehoseInstrumentation) {
        this.firehoseInstrumentation = firehoseInstrumentation;
    }

    @Override
    public List<Message> write(List<Message> messages) throws IOException {
        for (Message message : messages) {
            String key = message.getLogKey() == null ? "" : new String(message.getLogKey());
            String value = message.getLogMessage() == null ? "" : new String(message.getLogMessage());

            String error = "";
            ErrorInfo errorInfo = message.getErrorInfo();
            if (errorInfo != null) {
                if (errorInfo.getException() != null) {
                    error = ExceptionUtils.getStackTrace(errorInfo.getException());
                }
            }

            firehoseInstrumentation.logInfo("key: {}\nvalue: {}\nerror: {}", key, value, error);
        }
        return new LinkedList<>();
    }

}
