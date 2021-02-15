package com.gojek.esb.sink.jdbc;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.consumer.Message;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.AbstractSink;
import com.newrelic.api.agent.Trace;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * JDBC Sink allows messages consumed from kafka to be persisted to a database.
 * The related configurations for JDBC Sink can be found here: {@see com.gojek.esb.config.JdbcSinkConfig}
 */
public class JdbcSink extends AbstractSink {

    private JdbcConnectionPool pool;
    private QueryTemplate queryTemplate;
    private StencilClient stencilClient;
    private Statement statement;
    private Connection connection = null;

    public JdbcSink(Instrumentation instrumentation, String sinkType, JdbcConnectionPool pool, QueryTemplate queryTemplate, StencilClient stencilClient) {
        super(instrumentation, sinkType);
        this.pool = pool;
        this.queryTemplate = queryTemplate;
        this.stencilClient = stencilClient;
    }

    @Override
    protected void prepare(List<Message> messages) throws SQLException {
        List<String> queriesList = createQueries(messages);
        connection = pool.get();
        statement = connection.createStatement();

        for (String query : queriesList) {
            statement.addBatch(query);
        }
    }

    protected List<String> createQueries(List<Message> messages) {
        List<String> queries = new ArrayList<>();
        for (Message message : messages) {
            String queryString = queryTemplate.toQueryString(message);
            getInstrumentation().logDebug(queryString);
            queries.add(queryString);
        }
        return queries;
    }

    @Override
    @Trace(dispatcher = true)
    protected List<Message> execute() throws Exception {
        try {
            int[] updateCounts = statement.executeBatch();
            getInstrumentation().logDebug("DB response: {}", Arrays.toString(updateCounts));
        } finally {
            if (connection != null) {
                pool.release(connection);
            }
        }
        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        try {
            getInstrumentation().logInfo("Database connection closing");
            pool.shutdown();
            stencilClient.close();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
