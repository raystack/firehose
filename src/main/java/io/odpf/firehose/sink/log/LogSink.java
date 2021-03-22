package io.odpf.firehose.sink.log;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.Sink;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Sink implementation to write the messages read from kafka to the console.
 * The related configurations for LogSink can be found here: {@see io.odpf.firehose.config.LogConfig}
 */
@AllArgsConstructor
public class LogSink implements Sink {

    private KeyOrMessageParser parser;
    private Instrumentation instrumentation;

    /**
     * Push message list.
     *
     * @param messages the messages
     * @return the list
     * @throws IOException when invalid message is encountered
     */
    @Override
    public List<Message> pushMessage(List<Message> messages) throws IOException {
        for (Message message : messages) {
            instrumentation.logInfo("\n================= DATA =======================\n{}", parser.parse(message));
        }
        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
    }
}
