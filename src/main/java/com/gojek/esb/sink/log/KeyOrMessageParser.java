package com.gojek.esb.sink.log;

import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.AppConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.AllArgsConstructor;

import java.io.IOException;

@AllArgsConstructor
public class KeyOrMessageParser {

    private ProtoParser protoParser;
    private AppConfig appConfig;

    public DynamicMessage parse(EsbMessage message) throws IOException {
        if (appConfig.getKafkaRecordParserMode().equals("key")) {
            return protoParse(message.getLogKey());
        }
        return protoParse(message.getLogMessage());
    }

    private DynamicMessage protoParse(byte[] data) throws IOException {
        try {
            return protoParser.parse(data);
        } catch (InvalidProtocolBufferException e) {
            throw new IOException(e);
        }
    }
}
