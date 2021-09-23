package io.odpf.firehose.filter;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.TestKey;
import io.odpf.firehose.consumer.TestMessage;
import io.odpf.firehose.metrics.Instrumentation;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class NoOpFilterTest {

    @Mock
    private Instrumentation instrumentation;

    private NoOpFilter noOpFilter;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        noOpFilter = new NoOpFilter(instrumentation);
    }

    @Test
    public void shouldLogNoFilterSelected() {
        new NoOpFilter(instrumentation);
        verify(instrumentation, times(1)).logInfo("No filter is selected");
    }

    @Test
    public void shouldReturnInputListOfMessages() throws FilterException {
        TestKey testKeyProto1 = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        TestMessage testMessageProto1 = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();
        TestKey testKeyProto2 = TestKey.newBuilder().setOrderNumber("92").setOrderUrl("pqr").build();
        TestMessage testMessageProto2 = TestMessage.newBuilder().setOrderNumber("92").setOrderUrl("pqr").setOrderDetails("details").build();
        Message message1 = new Message(testKeyProto1.toByteArray(), testMessageProto1.toByteArray(), "topic1", 0, 100);
        Message message2 = new Message(testKeyProto2.toByteArray(), testMessageProto2.toByteArray(), "topic1", 0, 101);
        List<Message> inputMessages = Arrays.asList(message1, message2);
        List<Message> filteredMessages = noOpFilter.filter(Arrays.asList(message1, message2));
        assertEquals(inputMessages, filteredMessages);
    }
}
