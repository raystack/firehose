package com.gojek.esb.sink.log;

import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.sink.Sink;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Sink implementation to write the messages read from kafka to the console.
 * The related configurations for LogSink can be found here: {@see com.gojek.esb.config.LogConfig}
 */
public class LogSink implements Sink {

    private ProtoParser protoParser;
    private ProtoLogger protoLogger;
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(LogSink.class);

    public LogSink(ProtoParser protoParser, ProtoLogger protoLogger) {
        this.protoParser = protoParser;
        this.protoLogger = protoLogger;
    }

    @Override
    public List<EsbMessage> pushMessage(List<EsbMessage> esbMessages) throws IOException {
        for (EsbMessage message : esbMessages) {
            DynamicMessage key = null;
            byte[] logKey = message.getLogKey();
            if (logKey != null) {
                try {
                    key = parse(logKey);
                } catch (IOException e) {
                    LOGGER.warn("Unable to parse logKey", e);
                }
            }
            protoLogger.log(key, parse(message.getLogMessage()));
        }
        return new ArrayList<EsbMessage>();
    }

    private DynamicMessage parse(byte[] data) throws IOException {
        DynamicMessage dynamicMessage = null;
        try {
            dynamicMessage = protoParser.parse(data);
        } catch (InvalidProtocolBufferException e) {
            throw new IOException(e);
        }
        return dynamicMessage;
    }

    @Override
    public void close() throws IOException {
    }
}
