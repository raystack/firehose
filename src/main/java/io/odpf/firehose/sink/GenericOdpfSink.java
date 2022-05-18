package io.odpf.firehose.sink;

import io.odpf.depot.OdpfSink;
import io.odpf.depot.OdpfSinkResponse;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.message.FirehoseMessageUtils;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.FirehoseInstrumentation;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GenericOdpfSink extends AbstractSink {
    private final List<Message> messageList = new ArrayList<>();
    private final OdpfSink odpfSink;

    public GenericOdpfSink(FirehoseInstrumentation firehoseInstrumentation, String sinkType, OdpfSink odpfSink) {
        super(firehoseInstrumentation, sinkType);
        this.odpfSink = odpfSink;
    }

    @Override
    protected List<Message> execute() throws Exception {
        List<OdpfMessage> odpfMessages = FirehoseMessageUtils.convertToOdpfMessage(messageList);
        OdpfSinkResponse response = odpfSink.pushToSink(odpfMessages);
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

    }
}
