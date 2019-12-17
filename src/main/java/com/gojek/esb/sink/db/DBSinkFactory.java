package com.gojek.esb.sink.db;


import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.DBSinkConfig;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.sink.SinkFactory;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

/**
 * Factory class to create the DBSink.
 * <p>
 * The esb-log-consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map<String, String> configuration, StatsDClient client)}
 * to obtain the DBSink sink implementation.
 */
public class DBSinkFactory implements SinkFactory {

    public Sink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient client) {
        DBSinkConfig dbSinkConfig = ConfigFactory.create(DBSinkConfig.class, configuration);
        DBBatchCommand dbBatchCommand = createBatchCommand(dbSinkConfig);

        QueryTemplate queryTemplate = createQueryTemplate(dbSinkConfig, client);

        Instrumentation instrumentation = new Instrumentation(statsDReporter, DBSink.class);

        return new DBSink(dbBatchCommand, queryTemplate, instrumentation, client);
    }

    private DBBatchCommand createBatchCommand(DBSinkConfig dbSinkConfig) {
        long connectionTimeout = dbSinkConfig.getConnectionTimeout();
        long idleTimeout = dbSinkConfig.getIdleTimeout();
        DBConnectionPool connectionPool = new HikariDBConnectionPool(dbSinkConfig.getDbUrl(), dbSinkConfig.getUser(),
                dbSinkConfig.getPassword(), dbSinkConfig.getMaximumConnectionPoolSize(),
                connectionTimeout, idleTimeout, dbSinkConfig.getMinimumIdle());

        return new DBBatchCommand(connectionPool);
    }

    private QueryTemplate createQueryTemplate(DBSinkConfig dbSinkConfig, StencilClient stencilClient) {
        ProtoParser protoParser = new ProtoParser(stencilClient, dbSinkConfig.getProtoSchema());
        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(protoParser, dbSinkConfig.getProtoToFieldMapping());
        return new QueryTemplate(dbSinkConfig, protoToFieldMapper);
    }
}
