package com.gojek.esb.sink.db;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.Instrumentation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DBSinkTest {

    @Mock
    private QueryTemplate queryTemplate;

    private DBSink dbSink;

    @Mock
    private StencilClient stencilClient;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private DBConnectionPool dbConnectionPool;

    @Mock
    private Connection connection;

    @Mock
    private Statement statement;

    @Before
    public void setUp() throws SQLException {
        when(dbConnectionPool.get()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        dbSink = new DBSink(instrumentation, "db", dbConnectionPool, queryTemplate, stencilClient);
    }

    @Test
    public void shouldPopulateQueryString() throws IOException, DeserializerException, SQLException {
        EsbMessage esbMessage = new EsbMessage("key".getBytes(), "msg".getBytes(), "topic1", 0, 100);
        dbSink.pushMessage(Arrays.asList(esbMessage));

        verify(queryTemplate, times(1)).toQueryString(any(EsbMessage.class));
    }

    @Test
    public void shouldUseBatchForPushMessage() throws SQLException, IOException, DeserializerException {
        List<EsbMessage> esbMessages = Arrays.asList(new EsbMessage(new byte[0], new byte[0], "topic", 0, 100),
                new EsbMessage(new byte[0], new byte[0], "topic", 0, 100));
        dbSink.pushMessage(esbMessages);

        verify(instrumentation, times(1)).startExecution();
        verify(instrumentation, times(1)).captureSuccessExecutionTelemetry("db", esbMessages.size());
    }

    @Test
    public void shouldCallStartExecutionBeforeCaptureSuccessAtempt() throws IOException, DeserializerException, SQLException {
        List<EsbMessage> esbMessages = Arrays.asList(new EsbMessage(new byte[0], new byte[0], "topic", 0, 100),
                new EsbMessage(new byte[0], new byte[0], "topic", 0, 100));
        dbSink.pushMessage(esbMessages);

        verify(instrumentation, times(1)).startExecution();
        verify(instrumentation, times(1)).captureSuccessExecutionTelemetry("db", esbMessages.size());
        InOrder inOrder = inOrder(instrumentation);
        inOrder.verify(instrumentation).startExecution();
        inOrder.verify(instrumentation).captureSuccessExecutionTelemetry("db", esbMessages.size());
    }

    @Test
    public void shouldReturnEmptyListWhenNoException() throws IOException, DeserializerException {
        List<EsbMessage> esbMessages = Arrays.asList(new EsbMessage(new byte[0], new byte[0], "topic", 0, 100),
                new EsbMessage(new byte[0], new byte[0], "topic", 0, 100));

        assertEquals(dbSink.pushMessage(esbMessages).size(), 0);
        verify(instrumentation, times(1)).captureSuccessExecutionTelemetry("db", esbMessages.size());
    }

    @Test
    public void shouldReturnFailedMessagesWhenExecuteThrowsException() throws SQLException, IOException, DeserializerException {
        SQLException sqlException = new SQLException();
        when(connection.createStatement()).thenThrow(sqlException);
        List<EsbMessage> esbMessages = Arrays.asList(new EsbMessage(new byte[0], new byte[0], "topic", 0, 100),
                new EsbMessage(new byte[0], new byte[0], "topic", 0, 100));

        assertEquals(dbSink.pushMessage(esbMessages).size(), 2);
        verify(instrumentation, times(1)).captureFailedExecutionTelemetry(sqlException, esbMessages.size());
    }

    @Test
    public void shouldPrepareBatchForQueries() throws SQLException {
        List<String> queries = Arrays.asList("select * from table", "select count(*) from table");
        List<EsbMessage> esbMessages = Arrays.asList(new EsbMessage(new byte[0], new byte[0], "topic", 0, 100),
                new EsbMessage(new byte[0], new byte[0], "topic", 0, 100));

        DBSinkStub dbSinkStub = new DBSinkStub(instrumentation, "db", dbConnectionPool, queryTemplate, stencilClient, queries);

        dbSinkStub.prepare(esbMessages);
        verify(statement, times(queries.size())).addBatch(anyString());
    }

    @Test
    public void shouldReleaseConnectionAfterSuccessfulQuery() throws Exception {
        when(statement.executeBatch()).thenReturn(new int[]{});
        String sql = "select * from table";
        List<EsbMessage> esbMessages = Arrays.asList(new EsbMessage(new byte[0], new byte[0], "topic", 0, 100),
                new EsbMessage(new byte[0], new byte[0], "topic", 0, 100));
        DBSinkStub dbSinkStub = new DBSinkStub(instrumentation, "db", dbConnectionPool, queryTemplate, stencilClient, Arrays.asList(sql));

        dbSinkStub.pushMessage(esbMessages);
        verify(dbConnectionPool).release(connection);
    }

    @Test
    public void shouldReleaseConnectionOnException() throws Exception {
        List<EsbMessage> esbMessages = Arrays.asList(new EsbMessage(new byte[0], new byte[0], "topic", 0, 100),
                new EsbMessage(new byte[0], new byte[0], "topic", 0, 100));

        dbSink.pushMessage(esbMessages);
        verify(dbConnectionPool).release(connection);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAttemptConnectionReleaseIfPoolReturnsNull() throws SQLException, IOException, DeserializerException {
        when(dbConnectionPool.get()).thenReturn(null);
        List<EsbMessage> esbMessages = Arrays.asList(new EsbMessage(new byte[0], new byte[0], "topic", 0, 100),
                new EsbMessage(new byte[0], new byte[0], "topic", 0, 100));

        dbSink.pushMessage(esbMessages);
        verify(dbConnectionPool, never()).release(any());
    }

    @Test
    public void shouldCloseConnectionPool() throws IOException, InterruptedException {
        String sql = "select * from table";
        DBSinkStub dbSinkStub = new DBSinkStub(instrumentation, "db", dbConnectionPool, queryTemplate, stencilClient, Arrays.asList(sql));
        dbSinkStub.close();

        verify(dbConnectionPool, times(1)).shutdown();
    }

    @Test
    public void shouldCloseStencilClient() throws IOException {
        String sql = "select * from table";
        DBSinkStub dbSinkStub = new DBSinkStub(instrumentation, "db", dbConnectionPool, queryTemplate, stencilClient, Arrays.asList(sql));
        dbSinkStub.close();

        verify(stencilClient, times(1)).close();
    }

    @Test
    public void shouldLogWhenClosingConnection() throws IOException {
        String sql = "select * from table";
        DBSinkStub dbSinkStub = new DBSinkStub(instrumentation, "db", dbConnectionPool, queryTemplate, stencilClient, Arrays.asList(sql));
        dbSinkStub.close();

        verify(instrumentation, times(1)).logInfo("Database connection closing");
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenFailToClose() throws InterruptedException, IOException {
        doThrow(InterruptedException.class).when(dbConnectionPool).shutdown();

        List<String> queriesList = Arrays.asList("select * from table", "select * from table2");
        DBSinkStub dbSinkStub = new DBSinkStub(instrumentation, "db", dbConnectionPool, queryTemplate, stencilClient, queriesList);

        dbSinkStub.close();
    }

    @Test
    public void shouldLogQueryString() {
        EsbMessage esbMessage = new EsbMessage("key".getBytes(), "msg".getBytes(), "topic1", 0, 100);
        dbSink.createQueries(Arrays.asList(esbMessage));

        verify(instrumentation, times(1)).logDebug(queryTemplate.toQueryString(esbMessage));
    }

    @Test
    public void shouldLogDbResponse() throws Exception {
        int[] updateCounts = new int[]{};
        List<String> queries = Arrays.asList("select * from table");
        EsbMessage esbMessage = new EsbMessage(new byte[0], new byte[0], "topic", 0, 100);
        List<EsbMessage> esbMessages = Arrays.asList(esbMessage);

        when(statement.executeBatch()).thenReturn(updateCounts);
        DBSinkStub dbSinkStub = new DBSinkStub(instrumentation, "db", dbConnectionPool, queryTemplate, stencilClient, queries);

        dbSinkStub.pushMessage(esbMessages);

        verify(statement, times(1)).addBatch("select * from table");
        verify(instrumentation, times(1)).logDebug("Preparing {} messages", 1);
        verify(instrumentation, times(1)).logDebug("DB response: {}", Arrays.toString(updateCounts));
    }

    public class DBSinkStub extends DBSink {
        private List<String> queries;

        public DBSinkStub(Instrumentation instrumentation, String sinkType, DBConnectionPool pool, QueryTemplate queryTemplate, StencilClient stencilClient, List<String> queries) {
            super(instrumentation, sinkType, pool, queryTemplate, stencilClient);
            this.queries = queries;
        }

        protected List<String> createQueries(List<EsbMessage> messages) {
            return queries;
        }
    }
}
