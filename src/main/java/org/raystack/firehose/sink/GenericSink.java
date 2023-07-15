package org.raystack.firehose.sink;

import org.raystack.depot.Sink;
import org.raystack.depot.SinkResponse;
import org.raystack.firehose.exception.DeserializerException;
import org.raystack.firehose.message.FirehoseMessageUtils;
import org.raystack.firehose.message.Message;
import org.raystack.firehose.metrics.FirehoseInstrumentation;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GenericSink extends AbstractSink {
    private final List<Message> messageList = new ArrayList<>();
    private final Sink sink;

    public GenericSink(FirehoseInstrumentation firehoseInstrumentation, String sinkType, Sink sink) {
        super(firehoseInstrumentation, sinkType);
        this.sink = sink;
    }

    @Override
    protected List<Message> execute() throws Exception {
        List<org.raystack.depot.message.Message> messages = FirehoseMessageUtils.convertToDepotMessage(messageList);
        SinkResponse response = sink.pushToSink(messages);
        return response.getErrors().keySet().stream()
                .map(index -> {
                    Message message = messageList.get(index.intValue());
                    message.setErrorInfo(response.getErrorsFor(index));
                    return message;
                }).collect(Collectors.toList());
    }

    @Override
    protected void prepare(List<Message> messages) throws DeserializerException, IOException, SQLException {
        messageList.clear();
        messageList.addAll(messages);
    }

    @Override
    public void close() throws IOException {
        sink.close();
    }
}
