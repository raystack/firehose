package com.gojek.esb.sink;

import com.gojek.esb.consumer.EsbMessage;
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

    public List<EsbMessage> pushMessage(List<EsbMessage> esbMessages) throws DeserializerException {
        List<EsbMessage> failedMessages;
        try {
            instrumentation.logDebug("Preparing {} messages", esbMessages.size());
            prepare(esbMessages);
            instrumentation.capturePreExecutionLatencies(esbMessages);
            instrumentation.startExecution();
            failedMessages = execute();
            instrumentation.captureSuccessExecutionTelemetry(sinkType, esbMessages.size());
        } catch (DeserializerException | EglcConfigurationException | NullPointerException e) {
            throw e;
        } catch (Exception e) {
            if (!esbMessages.isEmpty()) {
                instrumentation.logWarn("Failed to push {} messages to sink", esbMessages.size());
            }
            instrumentation.captureFailedExecutionTelemetry(e, esbMessages.size());
            return esbMessages;
        }
        return failedMessages;
    }

    public Instrumentation getInstrumentation() {
        return instrumentation;
    }

    protected abstract List<EsbMessage> execute() throws Exception;

    protected abstract void prepare(List<EsbMessage> esbMessages) throws DeserializerException, IOException, SQLException;


}
