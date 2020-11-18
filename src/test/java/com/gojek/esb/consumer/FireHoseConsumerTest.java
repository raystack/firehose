package com.gojek.esb.consumer;

import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.filter.EsbFilterException;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.tracer.SinkTracer;
import com.gojek.esb.util.Clock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class FireHoseConsumerTest {

    @Mock
    private EsbGenericConsumer esbGenericConsumer;

    @Mock
    private Sink sink;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private Clock clock;

    @Mock
    private SinkTracer tracer;

    private FireHoseConsumer fireHoseConsumer;
    private List<EsbMessage> messages;

    @Before
    public void setUp() throws Exception {
        EsbMessage msg1 = new EsbMessage(new byte[]{}, new byte[]{}, "topic", 0, 100);
        EsbMessage msg2 = new EsbMessage(new byte[]{}, new byte[]{}, "topic", 0, 100);
        messages = Arrays.asList(msg1, msg2);

        fireHoseConsumer = new FireHoseConsumer(esbGenericConsumer, sink, clock, tracer, instrumentation);

        when(esbGenericConsumer.readMessages()).thenReturn(messages);
        when(clock.now()).thenReturn(Instant.now());
    }

    @Test
    public void shouldProcessPartitions() throws IOException, DeserializerException, EsbFilterException {
        fireHoseConsumer.processPartitions();

        verify(sink).pushMessage(messages);
    }

    @Test
    public void shouldProcessEmptyPartitions() throws IOException, DeserializerException, EsbFilterException {
        when(esbGenericConsumer.readMessages()).thenReturn(new ArrayList<>());

        fireHoseConsumer.processPartitions();

        verify(sink, times(0)).pushMessage(anyList());
    }

    @Test
    public void shouldSendNoOfMessagesReceivedCount() throws IOException, DeserializerException, EsbFilterException {
        fireHoseConsumer.processPartitions();
        verify(instrumentation).logInfo("Execution successful for {} records", 2);
    }

    @Test
    public void shouldSendPartitionProcessingTime() throws IOException, DeserializerException, EsbFilterException {
        Instant beforeCall = Instant.now();
        Instant afterCall = beforeCall.plusSeconds(1);
        when(clock.now()).thenReturn(beforeCall).thenReturn(afterCall);
        fireHoseConsumer.processPartitions();
        verify(instrumentation).captureDurationSince("kafka.process_partitions_time", beforeCall);
    }

    @Test
    public void shouldCallTracerWithTheSpan() throws IOException, DeserializerException, EsbFilterException {
        fireHoseConsumer.processPartitions();

        verify(sink).pushMessage(messages);
        verify(tracer).startTrace(messages);
        verify(tracer).finishTrace(any());
    }

    @Test
    public void shouldCloseConsumerIfConsumerIsNotNull() throws IOException {
        fireHoseConsumer.close();

        verify(instrumentation, times(1)).logInfo("closing consumer");
        verify(tracer, times(1)).close();
        verify(esbGenericConsumer, times(1)).close();

        verify(sink, times(1)).close();
        verify(instrumentation, times(1)).close();
    }

    @Test
    public void shouldNotCloseConsumerIfConsumerIsNull() throws IOException {

        fireHoseConsumer = new FireHoseConsumer(null, sink, clock, tracer, instrumentation);
        fireHoseConsumer.close();

        verify(instrumentation, times(0)).logInfo("closing consumer");
        verify(tracer, times(0)).close();
        verify(esbGenericConsumer, times(0)).close();

        verify(sink, times(1)).close();
        verify(instrumentation, times(1)).close();
    }
}
