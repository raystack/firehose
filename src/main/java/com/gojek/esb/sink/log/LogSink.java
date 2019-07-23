package com.gojek.esb.sink.log;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.sink.Sink;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Sink implementation to write the messages read from kafka to the console.
 * The related configurations for LogSink can be found here: {@see com.gojek.esb.config.LogConfig}
 */
@AllArgsConstructor
public class LogSink implements Sink {

    private KeyOrMessageParser parser;
    private ProtoLogger protoLogger;

    @Override
    public List<EsbMessage> pushMessage(List<EsbMessage> esbMessages) throws IOException {
        for (EsbMessage message : esbMessages) {
            protoLogger.log(parser.parse(message));
        }
        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
    }
}
