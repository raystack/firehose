package io.odpf.firehose.sink.log;

import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class LogSinkforJson extends AbstractSink {
    private List<Message> messageList;

    public LogSinkforJson(Instrumentation instrumentation) {
        super(instrumentation, "LOG");
    }

    @Override
    protected List<Message> execute() throws Exception {
        ArrayList<Message> invalidMessages = new ArrayList<>();
        JSONParser jsonParser = new JSONParser();
        for(Message m: messageList) {
            try {
                jsonParser.parse(new String(m.getLogMessage()));
            } catch (ParseException e) {
                invalidMessages.add(m);
            }
        }
        return invalidMessages;
    }

    @Override
    protected void prepare(List<Message> messages) throws DeserializerException, IOException, SQLException {
        this.messageList = messages;

    }

    @Override
    public void close() throws IOException {

    }
}
