package com.gojek.esb.sink;

import com.gojek.esb.consumer.Message;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.exception.EglcConfigurationException;
import com.gojek.esb.metrics.Instrumentation;
import lombok.AllArgsConstructor;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

@AllArgsConstructor
public abstract class AbstractSink implements Closeable, Sink {

    private Instrumentation instrumentation;
    private String sinkType;

    public List<Message> pushMessage(List<Message> messages) throws DeserializerException {
        List<Message> failedMessages;
        try {
            instrumentation.logDebug("Preparing {} messages", messages.size());
            prepare(messages);
            instrumentation.capturePreExecutionLatencies(messages);
            instrumentation.startExecution();
            failedMessages = execute();
            instrumentation.captureSuccessExecutionTelemetry(sinkType, messages.size());
        } catch (DeserializerException | EglcConfigurationException | NullPointerException e) {
            throw e;
        } catch (Exception e) {
            if (!messages.isEmpty()) {
                instrumentation.logWarn("Failed to push {} messages to sink", messages.size());
            }
            instrumentation.captureFailedExecutionTelemetry(e, messages.size());
            return messages;
        }
        return failedMessages;
    }

    public Instrumentation getInstrumentation() {
        return instrumentation;
    }

    protected abstract List<Message> execute() throws Exception;

    protected abstract void prepare(List<Message> messages) throws DeserializerException, IOException, SQLException;


}
