package io.odpf.firehose.sinkdecorator.dlq;

import io.odpf.firehose.consumer.Message;

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
}
