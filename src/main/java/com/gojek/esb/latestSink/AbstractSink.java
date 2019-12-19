package com.gojek.esb.latestSink;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.Sink;
import lombok.AllArgsConstructor;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
public abstract class AbstractSink implements Closeable, Sink {
    protected Instrumentation instrumentation;
    private String sinkType;

    public List<EsbMessage> pushMessage(List<EsbMessage> esbMessages) throws IOException, DeserializerException {
        List<EsbMessage> failedMessages = new ArrayList<>();
        prepare(esbMessages);
        try {
            instrumentation.lifetimeTillSink(esbMessages);
            instrumentation.startExecution();
            instrumentation.logInfo("pushing {} messages", esbMessages.size());
            failedMessages = execute();
            instrumentation.captureSuccessExecutionTelemetry(sinkType, esbMessages);
        } catch (Exception e) {
            instrumentation.captureFailedExecutionTelemetry(sinkType, e, esbMessages);
            return esbMessages;
        } finally {
            close();
        }
        return failedMessages;
    }

    protected abstract List<EsbMessage> execute() throws Exception;

    protected abstract void prepare(List<EsbMessage> esbMessages) throws DeserializerException, IOException;


}
