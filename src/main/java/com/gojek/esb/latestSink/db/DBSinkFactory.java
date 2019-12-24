package com.gojek.esb.latestSink.db;


import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.DBSinkConfig;
import com.gojek.esb.latestSink.AbstractSink;
import com.gojek.esb.latestSink.SinkFactory;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.proto.ProtoToFieldMapper;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

/**
 * Factory class to create the DBSink.
 * <p>
 * The esb-log-consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDClient client)}
 * to obtain the DBSink sink implementation.
 */
public class DBSinkFactory implements SinkFactory {

    public AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient client) {
        DBSinkConfig dbSinkConfig = ConfigFactory.create(DBSinkConfig.class, configuration);

        DBConnectionPool connectionPool = new HikariDBConnectionPool(dbSinkConfig.getDbUrl(), dbSinkConfig.getUser(),
                dbSinkConfig.getPassword(), dbSinkConfig.getMaximumConnectionPoolSize(),
                dbSinkConfig.getConnectionTimeout(), dbSinkConfig.getIdleTimeout(), dbSinkConfig.getMinimumIdle());

        QueryTemplate queryTemplate = createQueryTemplate(dbSinkConfig, client);

        Instrumentation instrumentation = new Instrumentation(statsDReporter, DBSink.class);

        return new DBSink(instrumentation, "db", connectionPool, queryTemplate, client);
    }


    private QueryTemplate createQueryTemplate(DBSinkConfig dbSinkConfig, StencilClient stencilClient) {
        ProtoParser protoParser = new ProtoParser(stencilClient, dbSinkConfig.getProtoSchema());
        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(protoParser, dbSinkConfig.getProtoToFieldMapping());
        return new QueryTemplate(dbSinkConfig, protoToFieldMapper);
    }
}
