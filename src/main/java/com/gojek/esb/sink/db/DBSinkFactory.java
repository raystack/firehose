package com.gojek.esb.sink.db;


import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.DBSinkConfig;
import com.gojek.esb.sink.AbstractSink;
import com.gojek.esb.sink.SinkFactory;
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

        Instrumentation instrumentation = new Instrumentation(statsDReporter, DBSinkFactory.class);
        String dbConfig = String.format(""
                        + "\n\tDB URL: %s\n\tDB Username: %s\n\tDB Tablename: %s\n\tMax connection pool size: %s\n\tConnection timeout: %s"
                        + "\n\tIdle timeout: %s\n\tMinimum idle: %s\n\tAudit enabled: %s\n\tUnique keys: %s",
                dbSinkConfig.getDbUrl(), dbSinkConfig.getUser(), dbSinkConfig.getTableName(), dbSinkConfig.getMaximumConnectionPoolSize(),
                dbSinkConfig.getConnectionTimeout(), dbSinkConfig.getIdleTimeout(), dbSinkConfig.getMinimumIdle(),
                dbSinkConfig.getAuditEnabled(), dbSinkConfig.getUniqueKeys());
        instrumentation.logDebug(dbConfig);
        DBConnectionPool connectionPool = new HikariDBConnectionPool(dbSinkConfig.getDbUrl(), dbSinkConfig.getUser(),
                dbSinkConfig.getPassword(), dbSinkConfig.getMaximumConnectionPoolSize(),
                dbSinkConfig.getConnectionTimeout(), dbSinkConfig.getIdleTimeout(), dbSinkConfig.getMinimumIdle());
        instrumentation.logInfo("DB Connection established");
        QueryTemplate queryTemplate = createQueryTemplate(dbSinkConfig, client);

        return new DBSink(new Instrumentation(statsDReporter, DBSink.class), "db", connectionPool, queryTemplate, client);
    }


    private QueryTemplate createQueryTemplate(DBSinkConfig dbSinkConfig, StencilClient stencilClient) {
        ProtoParser protoParser = new ProtoParser(stencilClient, dbSinkConfig.getProtoSchema());
        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(protoParser, dbSinkConfig.getProtoToFieldMapping());
        return new QueryTemplate(dbSinkConfig, protoToFieldMapper);
    }
}
