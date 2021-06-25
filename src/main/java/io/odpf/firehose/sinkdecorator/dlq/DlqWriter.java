package io.odpf.firehose.sinkdecorator.dlq;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.MessageWithError;

import java.io.IOException;
import java.util.List;

public interface DlqWriter {

    /**
     *
     * @param messages
     * @return
     * @throws IOException
     */
    List<Message> write(List<Message> messages) throws IOException;

    /**
     *
     * @param messages
     * @return messages failed to be sent to dlq
     */
    List<MessageWithError> writeWithError(List<MessageWithError> messages) throws IOException;
}
