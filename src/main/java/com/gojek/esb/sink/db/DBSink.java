package com.gojek.esb.sink.db;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.AbstractSink;
import com.newrelic.api.agent.Trace;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * DBSink allows messages consumed from kafka to be persisted to a database.
 * The related configurations for DBSink can be found here: {@see com.gojek.esb.config.DBSinkConfig}
 */
public class DBSink extends AbstractSink {

    private DBConnectionPool pool;
    private QueryTemplate queryTemplate;
    private StencilClient stencilClient;

    private List<String> queries;
    private Statement statement;
    private Connection connection = null;

    public DBSink(Instrumentation instrumentation, String sinkType, DBConnectionPool pool, QueryTemplate queryTemplate, StencilClient stencilClient) {
        super(instrumentation, sinkType);
        this.pool = pool;
        this.queryTemplate = queryTemplate;
        this.stencilClient = stencilClient;
    }

    @Override
    protected void prepare(List<EsbMessage> esbMessages) throws SQLException {
        List<String> queriesList = createQueries(esbMessages);
        connection = pool.get();
        statement = connection.createStatement();

        for (String query : queriesList) {
            statement.addBatch(query);
        }

    }

    protected List<String> createQueries(List<EsbMessage> messages) {
        queries = messages.stream()
                .map(esbMessage -> queryTemplate.toQueryString(esbMessage))
                .collect(Collectors.toList());
        return queries;
    }

    @Override
    @Trace(dispatcher = true)
    protected List<EsbMessage> execute() throws Exception {
        try {
            statement.executeBatch();
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
            pool.shutdown();
            stencilClient.close();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
