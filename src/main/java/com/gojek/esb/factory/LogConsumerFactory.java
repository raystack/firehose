package com.gojek.esb.factory;

import com.gojek.esb.audit.AuditMessageBuilder;
import com.gojek.esb.config.ApplicationConfiguration;
import com.gojek.esb.config.AuditConfig;
import com.gojek.esb.config.DBConfig;
import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.config.LogConfig;
import com.gojek.esb.config.SinkType;
import com.gojek.esb.consumer.EsbGenericConsumer;
import com.gojek.esb.consumer.LogConsumer;
import com.gojek.esb.server.AuditServiceResponseHandler;
import com.gojek.esb.sink.BackOffProvider;
import com.gojek.esb.sink.ExponentialBackOffProvider;
import com.gojek.esb.sink.HttpSink;
import com.gojek.esb.sink.db.ProtoToTableMapper;
import com.gojek.esb.sink.db.QueryTemplate;
import com.gojek.esb.sink.RetrySinkCommand;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.sink.db.DBBatchCommand;
import com.gojek.esb.sink.db.DBConnectionPool;
import com.gojek.esb.sink.db.DBSink;
import com.gojek.esb.sink.db.HikariDBConnectionPool;
import com.gojek.esb.sink.print.LogSink;
import com.gojek.esb.sink.print.ProtoParser;
import com.gojek.esb.util.TimeUtil;
import com.google.protobuf.GeneratedMessageV3;
import org.aeonbits.owner.ConfigFactory;
import org.asynchttpclient.DefaultAsyncHttpClient;

import java.util.List;
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
        } else if (appConfig.getSinkType() == SinkType.HTTP) {
            sink = new HttpSink(FactoryUtils.httpClient);
        } else {
            sink = getLogSink();
        }
        return new LogConsumer(consumer, sink, FactoryUtils.statsDClient, FactoryUtils.clockInstance);
    }

    private Sink getLogSink() {
        Sink sink;
        LogConfig logConfig = ConfigFactory.create(LogConfig.class, config);
        ProtoParser protoParser = new ProtoParser(logConfig.getProtoSchema());
        sink = new LogSink(protoParser, (List<GeneratedMessageV3> v) -> {
            v.forEach(System.out::println);
            System.out.println("===============================================");
        });
        return sink;
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
