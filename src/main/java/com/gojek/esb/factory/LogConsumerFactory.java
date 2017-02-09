package com.gojek.esb.factory;

import com.gojek.esb.audit.AuditMessageBuilder;
import com.gojek.esb.config.*;
import com.gojek.esb.consumer.EsbGenericConsumer;
import com.gojek.esb.consumer.LogConsumer;
import com.gojek.esb.server.AuditServiceResponseHandler;
import com.gojek.esb.sink.*;
import com.gojek.esb.sink.db.*;
import com.gojek.esb.sink.log.ConsoleLogger;
import com.gojek.esb.sink.log.LogSink;
import com.gojek.esb.sink.log.ProtoParser;
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
            sink = DBSink();
        } else if (appConfig.getSinkType() == SinkType.HTTP) {
            sink = new HttpSink(FactoryUtils.httpClient);
        } else {
            sink = LogSink();
        }
        return new LogConsumer(consumer, sink, FactoryUtils.statsDClient, FactoryUtils.clockInstance);
    }

    private Sink LogSink() {
        LogConfig logConfig = ConfigFactory.create(LogConfig.class, config);
        return new LogSink(new ProtoParser(logConfig.getProtoSchema()), new ConsoleLogger());
    }

    private Sink DBSink() {
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
                connectionTimeout, idleTimeout, dbConfig.getMinimumIdle());

        BackOffProvider backOffProvider = new ExponentialBackOffProvider(dbConfig.getInitialExpiryTimeInMs(),
                dbConfig.getBackOffRate(), dbConfig.getMaximumExpiryInMs());
        RetrySinkCommand retrySinkCommand = new RetrySinkCommand(backOffProvider);

        return new DBBatchCommand(retrySinkCommand, connectionPool);
    }

    private QueryTemplate createQueryTemplate(DBConfig dbConfig) {
        ProtoToTableMapper protoToTableMapper = new ProtoToTableMapper(dbConfig.getProtoSchema());
        return new QueryTemplate(dbConfig, protoToTableMapper);
    }
}
