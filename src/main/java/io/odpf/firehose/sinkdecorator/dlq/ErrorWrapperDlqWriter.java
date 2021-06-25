package io.odpf.firehose.sinkdecorator.dlq;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.MessageWithError;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public abstract class ErrorWrapperDlqWriter implements DlqWriter {

    @Override
    public List<Message> write(List<Message> messages) throws IOException {
        List<MessageWithError> messageWithErrors = wrapWithNoError(messages);
        List<MessageWithError> writeResult = writeWithError(messageWithErrors);
        return unwrapMessage(writeResult);
    }

    public static List<Message> unwrapMessage(List<MessageWithError> messageWithErrorsWriteResult) {
        return messageWithErrorsWriteResult.stream().map(MessageWithError::getMessage).collect(Collectors.toList());
    }

    public static List<MessageWithError> wrapWithNoError(List<Message> messages) {
        return messages.stream().map(message -> new MessageWithError(message, null)).collect(Collectors.toList());
    }
}
