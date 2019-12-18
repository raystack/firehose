package com.gojek.esb.sink.db;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.util.Clock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DBSinkTest {

    @Mock
    private DBBatchCommand dbBatchCommand;

    @Mock
    private QueryTemplate queryTemplate;

    private DBSink dbSink;

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private StencilClient stencilClient;

    @Mock
    private Instrumentation instrumentation;

    @Before
    public void setUp() {
        when(statsDReporter.getClock()).thenReturn(new Clock());
        dbSink = new DBSink(dbBatchCommand, queryTemplate, instrumentation, stencilClient);
    }

    @Test
    public void shouldPopulateQueryString() {
        EsbMessage esbMessage = new EsbMessage("key".getBytes(), "msg".getBytes(), "topic1", 0, 100);

        dbSink.pushMessage(Arrays.asList(esbMessage));

        verify(queryTemplate, times(1)).toQueryString(any(EsbMessage.class));
    }

    @Test
    public void shouldUseBatchForPushMessage() throws SQLException {
        List<EsbMessage> esbMessages = Arrays.asList(new EsbMessage(new byte[0], new byte[0], "topic", 0, 100),
                new EsbMessage(new byte[0], new byte[0], "topic", 0, 100));
        dbSink.pushMessage(esbMessages);
        List<String> upserts = esbMessages.stream().map(m -> queryTemplate.toQueryString(m)).collect(Collectors.toList());
        verify(dbBatchCommand, times(1)).execute(upserts);
        verify(instrumentation, times(1)).startExecution();
        verify(instrumentation, times(1)).captureSuccessExecutionTelemetry("db", esbMessages);
    }

    @Test
    public void shouldCallStartExecutionBeforeCaptureSuccessAtempt() {
        List<EsbMessage> esbMessages = Arrays.asList(new EsbMessage(new byte[0], new byte[0], "topic", 0, 100),
                new EsbMessage(new byte[0], new byte[0], "topic", 0, 100));
        dbSink.pushMessage(esbMessages);
        verify(instrumentation, times(1)).startExecution();
        verify(instrumentation, times(1)).captureSuccessExecutionTelemetry("db", esbMessages);
        InOrder inOrder = inOrder(instrumentation);
        inOrder.verify(instrumentation).startExecution();
        inOrder.verify(instrumentation).captureSuccessExecutionTelemetry("db", esbMessages);
    }

    @Test
    public void shouldReturnEmptyListWhenNoException() throws SQLException {
        List<EsbMessage> esbMessages = Arrays.asList(new EsbMessage(new byte[0], new byte[0], "topic", 0, 100),
                new EsbMessage(new byte[0], new byte[0], "topic", 0, 100));
        doNothing().when(dbBatchCommand).execute(anyList());
        assertEquals(dbSink.pushMessage(esbMessages).size(), 0);
        verify(instrumentation, times(1)).captureSuccessExecutionTelemetry("db", esbMessages);
    }

    @Test
    public void shouldReturnFailedMessagesWhenExecuteThrowsException() throws SQLException {
        SQLException sqlException = new SQLException();
        List<EsbMessage> esbMessages = Arrays.asList(new EsbMessage(new byte[0], new byte[0], "topic", 0, 100),
                new EsbMessage(new byte[0], new byte[0], "topic", 0, 100));
        doThrow(sqlException).when(dbBatchCommand).execute(anyList());
        assertEquals(dbSink.pushMessage(esbMessages).size(), 2);
        verify(instrumentation, times(1)).captureFailedExecutionTelemetry("db", sqlException, esbMessages);
    }
}
