package io.odpf.firehose.sink;

import io.odpf.depot.OdpfSink;
import io.odpf.depot.OdpfSinkResponse;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GenericOdpfSinkTest {

    private FirehoseInstrumentation instrumentation;
    private OdpfSink odpfSink;

    @Before
    public void setup() {
        instrumentation = Mockito.mock(FirehoseInstrumentation.class);
        odpfSink = Mockito.mock(OdpfSink.class);
    }

    @Test
    public void shouldReturnEmptyListOfMessagesWhenSuccess() throws Exception {
        GenericOdpfSink sink = new GenericOdpfSink(instrumentation, "test", odpfSink);
        Mockito.when(odpfSink.pushToSink(Mockito.anyList())).thenReturn(new OdpfSinkResponse());
        List<Message> messages = new ArrayList<Message>() {{
            Message m1 = new Message(new byte[1], new byte[1], "test", 1, 1);
            Message m2 = new Message(new byte[1], new byte[1], "test", 1, 2);
            Message m3 = new Message(new byte[1], new byte[1], "test", 1, 3);
            Message m4 = new Message(new byte[1], new byte[1], "test", 1, 4);
            Message m5 = new Message(new byte[1], new byte[1], "test", 1, 5);
            Message m6 = new Message(new byte[1], new byte[1], "test", 1, 6);
            add(m1);
            add(m2);
            add(m3);
            add(m4);
            add(m5);
            add(m6);
        }};
        sink.prepare(messages);
        List<Message> failedMessages = sink.execute();
        Assert.assertEquals(Collections.emptyList(), failedMessages);
    }

    @Test
    public void shouldReturnFailedMessages() throws Exception {
        GenericOdpfSink sink = new GenericOdpfSink(instrumentation, "test", odpfSink);
        OdpfSinkResponse response = new OdpfSinkResponse();
        response.addErrors(5, new ErrorInfo(new Exception(), ErrorType.SINK_4XX_ERROR));
        response.addErrors(2, new ErrorInfo(new Exception(), ErrorType.DEFAULT_ERROR));
        response.addErrors(4, new ErrorInfo(new Exception(), ErrorType.DESERIALIZATION_ERROR));
        Mockito.when(odpfSink.pushToSink(Mockito.anyList())).thenReturn(response);
        Message m1 = new Message(new byte[1], new byte[1], "test", 1, 1);
        Message m2 = new Message(new byte[1], new byte[1], "test", 1, 2);
        Message m3 = new Message(new byte[1], new byte[1], "test", 1, 3);
        Message m4 = new Message(new byte[1], new byte[1], "test", 1, 4);
        Message m5 = new Message(new byte[1], new byte[1], "test", 1, 5);
        Message m6 = new Message(new byte[1], new byte[1], "test", 1, 6);
        List<Message> messages = new ArrayList<Message>() {{
            add(m1);
            add(m2);
            add(m3);
            add(m4);
            add(m5);
            add(m6);
        }};
        sink.prepare(messages);
        List<Message> failedMessages = sink.execute();
        Assert.assertEquals(3, failedMessages.size());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, failedMessages.get(0).getErrorInfo().getErrorType());
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, failedMessages.get(1).getErrorInfo().getErrorType());
        Assert.assertEquals(ErrorType.SINK_4XX_ERROR, failedMessages.get(2).getErrorInfo().getErrorType());

        Assert.assertEquals(3, failedMessages.get(0).getOffset());
        Assert.assertEquals(5, failedMessages.get(1).getOffset());
        Assert.assertEquals(6, failedMessages.get(2).getOffset());
    }
}
