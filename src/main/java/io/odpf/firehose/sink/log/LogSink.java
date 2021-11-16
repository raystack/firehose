package io.odpf.firehose.sink.log;

import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Sink implementation to write the messages read from kafka to the console.
 * The related configurations for LogSink can be found here: {@see io.odpf.firehose.config.LogConfig}
 */
public class LogSink extends AbstractSink {

    private final KeyOrMessageParser parser;
    private final Instrumentation instrumentation;
    private final List<Message> messageList = new ArrayList<>();

    public LogSink(KeyOrMessageParser parser, Instrumentation instrumentation) {
        super(instrumentation, "LOG");
        this.parser = parser;
        this.instrumentation = instrumentation;
    }

    @Override
    protected List<Message> execute() throws Exception {
        for (Message message : messageList) {
            instrumentation.logInfo("\n================= DATA =======================\n{}", parser.parse(message));
        }
        return Collections.emptyList();
    }

    @Override
    protected void prepare(List<Message> messages) throws DeserializerException, IOException, SQLException {
        messageList.clear();
        messageList.addAll(messages);
    }

    @Override
    public void close() throws IOException {
    }
}
