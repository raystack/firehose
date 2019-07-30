package com.gojek.esb.consumer;

import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.filter.EsbFilterException;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.http.HttpSink;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FireHoseConsumerTest {

    @Mock
    private EsbGenericConsumer esbGenericConsumer;

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private HttpSink httpSink;

    @Mock
    private Clock clock;

    private FireHoseConsumer fireHoseConsumer;
    private List<EsbMessage> messages;

    @Before
    public void setUp() throws Exception {
        EsbMessage msg1 = new EsbMessage(new byte[]{}, new byte[]{}, "topic", 0, 100);
        EsbMessage msg2 = new EsbMessage(new byte[]{}, new byte[]{}, "topic", 0, 100);
        messages = Arrays.asList(msg1, msg2);

        fireHoseConsumer = new FireHoseConsumer(esbGenericConsumer, httpSink, statsDReporter, clock);

        when(esbGenericConsumer.readMessages()).thenReturn(messages);
        when(clock.now()).thenReturn(Instant.now());
    }

    @Test
    public void shouldProcessPartitions() throws IOException, DeserializerException, EsbFilterException {
        fireHoseConsumer.processPartitions();

        verify(httpSink).pushMessage(messages);
    }

    @Test
    public void shouldProcessEmptyPartitions() throws IOException, DeserializerException, EsbFilterException {
        when(esbGenericConsumer.readMessages()).thenReturn(new ArrayList<>());

        fireHoseConsumer.processPartitions();

        verify(httpSink, times(0)).pushMessage(anyList());
    }

    @Test
    public void shouldSendNoOfMessagesReceivedCount() throws IOException, DeserializerException, EsbFilterException {
        fireHoseConsumer.processPartitions();
        verify(statsDReporter).captureCount("kafka.messages.received", 2);
    }

    @Test
    public void shouldSendPartitionProcessingTime() throws IOException, DeserializerException, EsbFilterException {
        Instant beforeCall = Instant.now();
        Instant afterCall = beforeCall.plusSeconds(1);
        when(clock.now()).thenReturn(beforeCall).thenReturn(afterCall);
        fireHoseConsumer.processPartitions();
        verify(statsDReporter).captureDurationSince("kafka.process_partitions_time", beforeCall);
    }
}
