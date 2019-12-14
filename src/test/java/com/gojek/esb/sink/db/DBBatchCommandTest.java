package com.gojek.esb.sink.db;

import com.gojek.esb.sink.SinkCommandExecutionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DBBatchCommandTest {

    @Mock
    private DBConnectionPool dbConnectionPool;

    @Mock
    private Connection connection;

    @Mock
    private Statement statement;

    private DBBatchCommand batch;

    @Before
    public void setup() throws SQLException {
        when(statement.executeBatch()).thenReturn(new int[]{});
        when(connection.createStatement()).thenReturn(statement);
        when(dbConnectionPool.get()).thenReturn(connection);

        batch = new DBBatchCommand(dbConnectionPool);
    }

    @Test
    public void shouldExecuteASingleQuery() throws SQLException {
        String sql = "select * from table";

        batch.execute(Arrays.asList(sql));

        verify(statement, times(1)).addBatch(anyString());
        verify(statement, times(1)).executeBatch();
    }

    @Test
    public void shouldReleaseConnectionAfterSuccessfulQuery() throws SQLException {
        String sql = "select * from table";

        batch.execute(Arrays.asList(sql));

        verify(dbConnectionPool).release(connection);
    }

    @Test
    public void shouldExecuteMultipleQuery() throws SQLException {
        String[] sql = {"select * from table", "select * from table"};
        List<String> upserts = Arrays.stream(sql).collect(Collectors.toList());

        batch.execute(upserts);

        verify(statement, times(sql.length)).addBatch(anyString());
        verify(statement, times(1)).executeBatch();
    }

    @Test(expected = SQLException.class)
    public void shouldReleaseConnectionOnException() throws SQLException, SinkCommandExecutionException {
        doThrow(SQLException.class)
                .doReturn(new int[]{1})
                .when(statement).executeBatch();
        String[] sql = {"select * from table", "select * from table"};
        List<String> upserts = Arrays.stream(sql).collect(Collectors.toList());

        batch.execute(upserts);

        verify(dbConnectionPool, times(1)).release(connection);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAttemptConnectionReleaseIfPoolReturnsNull() throws SQLException, SinkCommandExecutionException {
        batch = new DBBatchCommand(dbConnectionPool);
        when(dbConnectionPool.get()).thenReturn(null);
        String[] sql = {"select * from table", "select * from table"};
        List<String> upserts = Arrays.stream(sql).collect(Collectors.toList());

        batch.execute(upserts);

        verify(dbConnectionPool, never()).release(any());
    }
}
