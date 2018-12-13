package com.gojek.esb.sink.db;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.util.Clock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
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

    @Before
    public void setUp() {
        when(statsDReporter.getClock()).thenReturn(new Clock());
        dbSink = new DBSink(dbBatchCommand, queryTemplate, statsDReporter);
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
        verify(statsDReporter, times(1)).captureCount(any(), any(), any());
        verify(statsDReporter, times(1)).captureDurationSince(any(), any(), any());

    }

    @Test
    public void shouldReturnEmptyListWhenNoException() throws SQLException {
        List<EsbMessage> esbMessages = Arrays.asList(new EsbMessage(new byte[0], new byte[0], "topic", 0, 100),
                new EsbMessage(new byte[0], new byte[0], "topic", 0, 100));
        doNothing().when(dbBatchCommand).execute(anyList());
        assertEquals(dbSink.pushMessage(esbMessages).size(), 0);
        verify(statsDReporter, times(1)).captureCount(any(), any(), any());
        verify(statsDReporter, times(1)).captureDurationSince(any(), any(), any());
    }

    @Test
    public void shouldReturnFailedMessagesWhenExecuteThrowsException() throws SQLException {
        List<EsbMessage> esbMessages = Arrays.asList(new EsbMessage(new byte[0], new byte[0], "topic", 0, 100),
                new EsbMessage(new byte[0], new byte[0], "topic", 0, 100));
        doThrow(new SQLException()).when(dbBatchCommand).execute(anyList());
        assertEquals(dbSink.pushMessage(esbMessages).size(), 2);
        verify(statsDReporter, times(1)).captureCount(any(), any(), any());
        verify(statsDReporter, times(0)).captureDurationSince(any(), any(), any());
    }
}
