package com.gojek.esb.sink.db;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.sink.Sink;

import lombok.AllArgsConstructor;

/**
 * DBSink allows messages consumed from kafka to be persisted to a database.
 * The related configurations for DBSink can be found here: {@see com.gojek.esb.config.DBSinkConfig}
 */
@AllArgsConstructor
public class DBSink implements Sink {

    private DBBatchCommand dbBatchCommand;
    private QueryTemplate queryTemplate;
    private Instrumentation instrumentation;
    private StencilClient stencilClient;

    @Override
    public List<EsbMessage> pushMessage(List<EsbMessage> esbMessages) {
        List<String> queries = esbMessages.stream()
                .map(esbMessage -> queryTemplate.toQueryString(esbMessage))
                .collect(Collectors.toList());
        try {
            instrumentation.startExecution();
            dbBatchCommand.execute(queries);
            instrumentation.captureSuccessAtempt(esbMessages);
        } catch (SQLException e) {
            instrumentation.captureFailedAttempt(e, esbMessages);
            return esbMessages;
        }
        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        try {
            dbBatchCommand.shutdownPool();
            stencilClient.close();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
