package io.odpf.firehose.sink.clickhouse;

import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.ClickHouseClient;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.Instrumentation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


@RunWith(MockitoJUnitRunner.class)
public class ClickhouseSinkTest {

    @Mock
    private QueryTemplate queryTemplate;

    @Mock
    private Instrumentation instrumentation;

    private ClickhouseSink clickhouseSink;

    @Mock
    private CompletableFuture<ClickHouseResponse> future;

    @Mock
    private ClickHouseResponse clickHouseResponse;

    @Mock
    private ClickHouseRequest request;

    @Mock
    private ClickHouseResponseSummary clickHouseResponseSummary;

    @Mock
    private ClickHouseClient clickHouseClient;

    @Before
    public void setUp() {
        when(instrumentation.startExecution()).thenReturn(Instant.now());
        clickhouseSink = new ClickhouseSink(instrumentation, request, queryTemplate, clickHouseClient);
    }

    @Test
    public void shouldPopulateQueryString() throws Exception {
        Message message = new Message("key".getBytes(), "msg".getBytes(), "topic1", 0, 100);
        when(queryTemplate.toQueryStringForMultipleMessages(any(List.class))).thenReturn("query");
        when(request.execute()).thenReturn(future);

        when(future.get()).thenReturn(clickHouseResponse);
        when(clickHouseResponse.getSummary()).thenReturn(clickHouseResponseSummary);
        when(request.query(anyString())).thenReturn(request);
        when(clickHouseResponseSummary.getWrittenRows()).thenReturn(2l);

        clickhouseSink.pushMessage(Arrays.asList(message));
        verify(queryTemplate, times(1)).toQueryStringForMultipleMessages(any(List.class));
        clickhouseSink.close();
    }

    @Test
    public void testExecuteMethodException() throws Exception {
        Message message = new Message("key".getBytes(), "msg".getBytes(), "topic1", 0, 100);
        when(queryTemplate.toQueryStringForMultipleMessages(any(List.class))).thenReturn("query");
        when(request.execute()).thenReturn(future);
        when(request.query(anyString())).thenReturn(request);
        Throwable t = new IOException("test");
        when(future.get()).thenThrow(new ExecutionException(t));
        clickhouseSink.prepare(Arrays.asList(message));
        Assert.assertEquals(Arrays.asList(message), clickhouseSink.execute());
        clickhouseSink.close();
    }
}