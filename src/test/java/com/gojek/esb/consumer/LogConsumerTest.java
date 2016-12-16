package com.gojek.esb.consumer;

import com.gojek.esb.util.Clock;
import com.gojek.esb.client.GenericHTTPClient;
import com.timgroup.statsd.StatsDClient;
import org.apache.http.HttpResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LogConsumerTest {

    @Mock
    private EsbGenericConsumer esbGenericConsumer;

    @Mock
    private GenericHTTPClient genericHTTPClient;

    @Mock
    private HttpResponse httpResponse;

    @Mock
    private StatsDClient statsDClient;

    @Mock
    private Clock clock;

    private LogConsumer logConsumer;
    private List<EsbMessage> messages;

    @Before
    public void setUp() throws Exception {
        EsbMessage msg1 = new EsbMessage(new byte[]{}, new byte[]{}, "topic");
        EsbMessage msg2 = new EsbMessage(new byte[]{}, new byte[]{}, "topic");
        messages = Arrays.asList(msg1, msg2);

        logConsumer = new LogConsumer(esbGenericConsumer, genericHTTPClient, statsDClient, clock);

        when(esbGenericConsumer.readMessages()).thenReturn(messages);
        when(genericHTTPClient.execute(any(List.class))).thenReturn(httpResponse);
        when(clock.now()).thenReturn(Instant.now());
    }

    @Test
    public void shouldProcessPartitions() throws IOException {
        logConsumer.processPartitions();

        verify(genericHTTPClient).execute(messages);
    }

    @Test
    public void shouldProcessEmptyPartitions() throws IOException {
        when(esbGenericConsumer.readMessages()).thenReturn(new ArrayList<>());

        logConsumer.processPartitions();

        verify(genericHTTPClient, times(0)).execute(any(List.class));
    }

    @Test
    public void shouldSendNoOfMessagesReceivedCount() throws IOException {
        logConsumer.processPartitions();
        verify(statsDClient).count("messages.received", 2);
    }

    @Test
    public void shouldSendPartitionProcessingTime() throws IOException {
        Instant beforeCall = Instant.now();
        Instant afterCall = beforeCall.plusSeconds(1);
        when(clock.now()).thenReturn(beforeCall).thenReturn(afterCall);
        logConsumer.processPartitions();
        verify(statsDClient).recordExecutionTime("messages.process_partitions_time", Duration.between(beforeCall, afterCall).toMillis());
    }

    @Test
    public void shouldSendBatchSize() throws IOException {
        logConsumer.processPartitions();
        verify(statsDClient).gauge("messages.batch.size", 2);
    }
}