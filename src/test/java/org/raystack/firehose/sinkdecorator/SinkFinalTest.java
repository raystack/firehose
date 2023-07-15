package org.raystack.firehose.sinkdecorator;

import org.raystack.firehose.message.Message;
import org.raystack.firehose.metrics.FirehoseInstrumentation;
import org.raystack.firehose.metrics.Metrics;
import org.raystack.firehose.sink.Sink;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SinkFinalTest {

    @Test
    public void shouldIgnoreMessages() throws IOException {
        Sink sink = Mockito.mock(Sink.class);
        FirehoseInstrumentation firehoseInstrumentation = Mockito.mock(FirehoseInstrumentation.class);
        SinkFinal sinkFinal = new SinkFinal(sink, firehoseInstrumentation);
        List<Message> messages = new ArrayList<Message>() {{
            add(new Message("".getBytes(), "".getBytes(), "", 0, 0));
            add(new Message("".getBytes(), "".getBytes(), "", 0, 0));
        }};
        Mockito.when(sink.pushMessage(messages)).thenReturn(messages);

        sinkFinal.pushMessage(messages);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).logInfo("Ignoring messages {}", 2);
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).captureGlobalMessageMetrics(Metrics.MessageScope.IGNORED, 2);
    }
}
