package com.gojek.esb.factory;

import com.gojek.esb.audit.AuditMessageBuilder;
import com.gojek.esb.config.ApplicationConfiguration;
import com.gojek.esb.config.AuditConfig;
import com.gojek.esb.config.DBConfig;
import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.config.SinkType;
import com.gojek.esb.consumer.EsbGenericConsumer;
import com.gojek.esb.consumer.LogConsumer;
import com.gojek.esb.server.AuditServiceResponseHandler;
import com.gojek.esb.sink.BackOffProvider;
import com.gojek.esb.sink.ExponentialBackOffProvider;
import com.gojek.esb.sink.HttpSink;
import com.gojek.esb.sink.ProtoToTableMapper;
import com.gojek.esb.sink.QueryTemplate;
import com.gojek.esb.sink.RetrySinkCommand;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.sink.db.DBBatchCommand;
import com.gojek.esb.sink.db.DBConnectionPool;
import com.gojek.esb.sink.db.DBSink;
import com.gojek.esb.sink.db.HikariDBConnectionPool;
import com.gojek.esb.util.TimeUtil;
import org.aeonbits.owner.ConfigFactory;
import org.asynchttpclient.DefaultAsyncHttpClient;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

public class LogConsumerFactory {

    private Map<String, String> config;
    private final ApplicationConfiguration appConfig;

    public LogConsumerFactory(Map<String, String> config) {

        this.config = config;

        appConfig = ConfigFactory.create(ApplicationConfiguration.class, config);

    }

    public static LogConsumer getLogConsumer(Sink sink) {
        return new LogConsumer(null, sink, FactoryUtils.statsDClient, FactoryUtils.clockInstance);
    }

    public LogConsumer getConsumer() {
        KafkaConsumerConfig kafkaConsumerConfig = new KafkaConsumerConfig(appConfig.getKafkaAddress(),
                appConfig.getConsumerGroupId(),
                Pattern.compile(appConfig.getKafkaTopic()),
                Long.MAX_VALUE,
                config
        );

        AuditConfig auditConfig = new AuditConfig(new DefaultAsyncHttpClient(), kafkaConsumerConfig.getGroupId(),
                appConfig.getAuditServiceUrl(), appConfig.isAuditEnabled(), Optional.of(new AuditServiceResponseHandler()), Optional.of(new AuditMessageBuilder(new TimeUtil())));
        EsbGenericConsumer consumer = new GenericKafkaFactory().createConsumer(kafkaConsumerConfig, auditConfig);

        Sink sink;
        if (appConfig.getSinkType() == SinkType.DB) {
            sink = createDBSink();
        } else {
            sink = new HttpSink(FactoryUtils.httpClient);
        }
        return new LogConsumer(consumer, sink, FactoryUtils.statsDClient, FactoryUtils.clockInstance);
    }

    private Sink createDBSink() {
        DBConfig dbConfig = ConfigFactory.create(DBConfig.class, config);
        DBBatchCommand dbBatchCommand = createBatchCommand(dbConfig);
        QueryTemplate queryTemplate = createQueryTemplate(dbConfig);

        return new DBSink(dbBatchCommand, queryTemplate);
    }

    private DBBatchCommand createBatchCommand(DBConfig dbConfig) {
        long connectionTimeout = dbConfig.getConnectionTimeout();
        long idleTimeout = dbConfig.getIdleTimeout();
        DBConnectionPool connectionPool = new HikariDBConnectionPool(dbConfig.getDbUrl(), dbConfig.getUser(),
                dbConfig.getPassword(), dbConfig.getMaximumConnectionPoolSize(),
                connectionTimeout, idleTimeout);

        BackOffProvider backOffProvider = new ExponentialBackOffProvider(dbConfig.getInitialExpiryTimeInMs(),
                dbConfig.getBackOffRate(), dbConfig.getMaximumExpiryInMs());
        RetrySinkCommand retrySinkCommand = new RetrySinkCommand(backOffProvider);

        return new DBBatchCommand(connectionPool, retrySinkCommand);
    }

    private QueryTemplate createQueryTemplate(DBConfig dbConfig) {
        ProtoToTableMapper protoToTableMapper = new ProtoToTableMapper(dbConfig.getProtoSchema());
        return new QueryTemplate(dbConfig, protoToTableMapper);
    }
}
