package io.odpf.firehose.sink;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.MessageWithError;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.exception.EglcConfigurationException;
import io.odpf.firehose.metrics.Instrumentation;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * Abstract sink.
 * All other type of sink will implement this.
 */
@AllArgsConstructor
public abstract class AbstractSink implements Sink, DlqProcessor {

    private final Instrumentation instrumentation;
    private final String sinkType;

    /**
     * Method to push messages to sink.
     *
     * @param messages the messages
     * @return the list
     * @throws DeserializerException when invalid kafka message is encountered
     */
    public List<Message> pushMessage(List<Message> messages) throws DeserializerException {
        List<Message> failedMessages;
        try {
            instrumentation.logDebug("Preparing {} messages", messages.size());
            prepare(messages);
            instrumentation.capturePreExecutionLatencies(messages);
            instrumentation.startExecution();
            ExecResult execResult = executeWithError();
            List<MessageWithError> dlqResult = processDlq(execResult.getDeadLetterQueue());
            instrumentation.logWarn("Failed to push {} messages to dlq", dlqResult.size());
            failedMessages = execResult.getRetryAbleMessages();
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

    @Override
    public ExecResult executeWithError() throws Exception {
        return new ExecResult(execute(), new LinkedList<>());
    }

    /**
     * Gets instrumentation.
     *
     * @return the instrumentation
     */
    public Instrumentation getInstrumentation() {
        return instrumentation;
    }

    /**
     * send messages to the sink.
     *
     * @return the list
     * @throws Exception the exception
     */
    protected abstract List<Message> execute() throws Exception;

    /**
     * process the messages before sending to the sink.
     *
     * @param messages the messages
     * @throws DeserializerException the deserializer exception
     * @throws IOException           the io exception
     * @throws SQLException          the sql exception
     */
    protected abstract void prepare(List<Message> messages) throws DeserializerException, IOException, SQLException;

    @Override
    public List<MessageWithError> processDlq(List<MessageWithError> messages) throws IOException {
        return new LinkedList<>();
    }
}
