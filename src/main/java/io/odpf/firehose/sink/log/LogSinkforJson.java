package io.odpf.firehose.sink.log;

import io.odpf.firehose.error.ErrorInfo;
import io.odpf.firehose.error.ErrorType;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class LogSinkforJson extends AbstractSink {
    private List<Message> messageList;
    private Instrumentation instrumentation;

    public LogSinkforJson(Instrumentation instrumentation) {
        super(instrumentation, "LOG");
        this.instrumentation = instrumentation;
    }

    @Override
    protected List<Message> execute() throws Exception {
        ArrayList<Message> invalidMessages = new ArrayList<>();
        for (Message m : messageList) {
            try {
                JSONObject jsonObject = new JSONObject(new String(m.getLogMessage()));
                instrumentation.logInfo("\n================= DATA =======================\n{}", jsonObject);
            } catch (JSONException ex) {
                m.setErrorInfo(new ErrorInfo(ex, ErrorType.DESERIALIZATION_ERROR));
                invalidMessages.add(m);
            }
        }
        return invalidMessages;
    }

    @Override
    protected void prepare(List<Message> messages) throws IOException, SQLException {
        this.messageList = messages;

    }

    @Override
    public void close() throws IOException {

    }
}
