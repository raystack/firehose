package com.gojek.esb.sink.db;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.Sink;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.gojek.esb.metrics.Metrics.DB_SINK_MESSAGES_COUNT;
import static com.gojek.esb.metrics.Metrics.DB_SINK_WRITE_TIME;
import static com.gojek.esb.metrics.Metrics.FAILURE_TAG;
import static com.gojek.esb.metrics.Metrics.SUCCESS_TAG;

/**
 * DBSink allows messages consumed from kafka to be persisted to a database.
 * The related configurations for DBSink can be found here: {@see com.gojek.esb.config.DBSinkConfig}
 */
@AllArgsConstructor
public class DBSink implements Sink {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBSink.class);
    private DBBatchCommand dbBatchCommand;
    private QueryTemplate queryTemplate;
    private StatsDReporter statsDReporter;
    private StencilClient stencilClient;

    @Override
    public List<EsbMessage> pushMessage(List<EsbMessage> esbMessages) {
        List<String> queries = esbMessages.stream()
                .map(esbMessage -> queryTemplate.toQueryString(esbMessage))
                .collect(Collectors.toList());
        try {
            Instant startExecution = statsDReporter.getClock().now();
            dbBatchCommand.execute(queries);
            statsDReporter.captureDurationSince(DB_SINK_WRITE_TIME, startExecution);
            statsDReporter.captureCount(DB_SINK_MESSAGES_COUNT, esbMessages.size(), SUCCESS_TAG);
        } catch (SQLException e) {
            LOGGER.error("caught {} {}", e.getClass(), e.getMessage());
            statsDReporter.captureCount(DB_SINK_MESSAGES_COUNT, esbMessages.size(), FAILURE_TAG);
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
