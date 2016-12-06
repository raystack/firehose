package com.gojek.esb.consumer;

import com.gojek.esb.client.GenericHTTPClient;
import org.apache.http.HttpResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Matchers.any;
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

        when(esbGenericConsumer.readMessages()).thenReturn(messages);
        when(genericHTTPClient.execute(any(List.class))).thenReturn(httpResponse);

        logConsumer = new LogConsumer(esbGenericConsumer, genericHTTPClient);
    }

    @Test
    public void shouldCallServiceURL() throws IOException {
        logConsumer.processPartitions();

        verify(genericHTTPClient).execute(messages);
    }

}