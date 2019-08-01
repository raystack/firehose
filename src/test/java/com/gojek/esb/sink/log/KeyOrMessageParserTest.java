package com.gojek.esb.sink.log;

import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.AppConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.consumer.TestMessage;
import com.google.protobuf.DynamicMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class KeyOrMessageParserTest {

    @Mock
    private AppConfig appConfig;

    @Mock
    private ProtoParser protoParser;

    private DynamicMessage dynamicMessage;

    private EsbMessage esbMessage;

    private KeyOrMessageParser parser;

    @Before
    public void setup() throws IOException {
        dynamicMessage = DynamicMessage.newBuilder(TestMessage.getDescriptor()).build();

        Mockito.when(appConfig.getKafkaRecordParserMode()).thenReturn("message");
        Mockito.when(protoParser.parse(Mockito.any(byte[].class))).thenReturn(dynamicMessage);

        esbMessage = new EsbMessage("logKey".getBytes(), "logMessage".getBytes(), "topic", 0, 10);
        parser = new KeyOrMessageParser(protoParser, appConfig);
    }

    @Test
    public void shouldParseMessageByDefault() throws IOException {
        parser.parse(esbMessage);

        Mockito.verify(protoParser, Mockito.times(1)).parse("logMessage".getBytes());
    }

    @Test
    public void shouldParseKeyWhenKafkaMessageParserModeSetToKey() throws IOException {
        Mockito.when(appConfig.getKafkaRecordParserMode()).thenReturn("key");
        parser.parse(esbMessage);

        Mockito.verify(protoParser, Mockito.times(1)).parse("logKey".getBytes());
    }

}