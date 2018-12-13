package com.gojek.esb.sink.db;

import com.newrelic.api.agent.Trace;
import lombok.AllArgsConstructor;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * class to run a batch of queries against a database.
 */
@AllArgsConstructor
public class DBBatchCommand implements DBCommand {

    private DBConnectionPool pool;

    @Override
    @Trace(dispatcher = true)
    public void execute(List<String> queries) throws SQLException {
        Connection connection = null;
        try {
            connection = pool.get();
            Statement statement = connection.createStatement();

            for (String query : queries) {
                statement.addBatch(query);
            }

            statement.executeBatch();
        } finally {
            if (connection != null) {
                pool.release(connection);
            }
        }
    }

    public void shutdownPool() throws InterruptedException {
        pool.shutdown();
    }
}
