package com.gojek.esb.sink.log;

import com.gojek.esb.consumer.Message;
import com.gojek.esb.consumer.TestKey;
import com.gojek.esb.consumer.TestMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.Sink;
import com.google.protobuf.DynamicMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class LogSinkTest {

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private KeyOrMessageParser parser;

    private DynamicMessage dynamicMessage = DynamicMessage.newBuilder(TestMessage.getDescriptor()).build();

    private Sink sink;

    @Before
    public void setup() throws IOException {
        Mockito.when(parser.parse(Mockito.any(Message.class))).thenReturn(dynamicMessage);

        sink = new LogSink(parser, instrumentation);
    }

    @Test
    public void shouldPrintProto() throws IOException, DeserializerException {
        List<Message> messages = Arrays.asList(new Message(new byte[0], new byte[0], "topic", 0, 100));

        sink.pushMessage(messages);

        Mockito.verify(instrumentation, Mockito.times(1)).logInfo(
                Mockito.eq("\n================= DATA =======================\n{}"),
                Mockito.any(DynamicMessage.class));
    }

    @Test
    public void shouldParseProto() throws IOException, InvocationTargetException, IllegalAccessException, DeserializerException {
        List<Message> messages = Arrays.asList(new Message(new byte[0], new byte[0], "topic", 0, 100),
                new Message(new byte[0], new byte[0], "topic-2", 0, 100));

        sink.pushMessage(messages);

        Mockito.verify(parser, Mockito.times(2)).parse(Mockito.any(Message.class));
    }

    @Test
    public void shouldPrintTestProto() throws IOException, DeserializerException {
        TestKey testKey = TestKey.getDefaultInstance();
        TestMessage testMessage = TestMessage.getDefaultInstance();
        List<Message> messages = Arrays.asList(new Message(testKey.toByteArray(), testMessage.toByteArray(), "topic", 0, 100));

        sink.pushMessage(messages);

        Mockito.verify(instrumentation, Mockito.times(1)).logInfo(
                Mockito.eq("\n================= DATA =======================\n{}"),
                Mockito.any(DynamicMessage.class));
    }

    @Test
    public void shouldSkipParsingAndNotFailIfKeyIsNull() throws IOException, DeserializerException, InvocationTargetException, IllegalAccessException {
        byte[] testMessage = TestMessage.getDefaultInstance().toByteArray();

        sink.pushMessage(Arrays.asList(new Message(null, testMessage, "topic", 0, 100)));

        Mockito.verify(instrumentation, Mockito.times(1)).logInfo(
                Mockito.eq("\n================= DATA =======================\n{}"),
                Mockito.any(DynamicMessage.class));
        Mockito.verify(parser, Mockito.never()).parse(null);
    }
}
