package com.gojek.esb.consumer;

import com.gojek.esb.client.GenericHTTPClient;
import org.apache.http.HttpResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
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

    private LogConsumer logConsumer;
    private List<EsbMessage> messages;

    @Before
    public void setUp() throws Exception {
        EsbMessage msg = new EsbMessage(new byte[]{}, new byte[]{}, "topic");
        messages = Arrays.asList(msg);

        when(genericHTTPClient.execute(any(List.class))).thenReturn(httpResponse);
    }

    @Test
    public void shouldProcessPartitions() throws IOException {
        when(esbGenericConsumer.readMessages()).thenReturn(messages);
        logConsumer = new LogConsumer(esbGenericConsumer, genericHTTPClient);

        logConsumer.processPartitions();

        verify(genericHTTPClient).execute(messages);
    }

    @Test
    public void shouldProcessEmptyPartitions() throws IOException {
        when(esbGenericConsumer.readMessages()).thenReturn(new ArrayList<>());
        logConsumer = new LogConsumer(esbGenericConsumer, genericHTTPClient);

        logConsumer.processPartitions();

        verify(genericHTTPClient, times(0)).execute(any(List.class));
    }
}