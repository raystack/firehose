package com.gojek.esb.factory;

import com.gojek.esb.audit.AuditMessageBuilder;
import com.gojek.esb.client.BaseHttpClient;
import com.gojek.esb.client.ExponentialBackoffClient;
import com.gojek.esb.client.GenericHTTPClient;
import com.gojek.esb.config.*;
import com.gojek.esb.consumer.EsbGenericConsumer;
import com.gojek.esb.consumer.LogConsumer;
import com.gojek.esb.parser.Header;
import com.gojek.esb.server.AuditServiceResponseHandler;
import com.gojek.esb.sink.*;
import com.gojek.esb.sink.db.*;
import com.gojek.esb.sink.log.ConsoleLogger;
import com.gojek.esb.sink.log.LogSink;
import com.gojek.esb.sink.log.ProtoParser;
import com.gojek.esb.util.Clock;
import com.gojek.esb.util.TimeUtil;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

public class LogConsumerFactory {

    private Map<String, String> config;
    private final ApplicationConfiguration appConfig;
    private final HTTPSinkConfig httpSinkConfig;
    private final StatsDClient statsDClient;
    private final Clock clockInstance;
    private static final Logger logger = LoggerFactory.getLogger(LogConsumerFactory.class);

    public LogConsumerFactory(Map<String, String> config) {
        this.config = config;
        appConfig = ConfigFactory.create(ApplicationConfiguration.class, config);
        logger.info("--------- Config ---------");
        logger.info(appConfig.getKafkaAddress());
        logger.info(appConfig.getKafkaTopic());
        logger.info(appConfig.getConsumerGroupId());
        logger.info(appConfig.getSinkType().name());
        logger.info("--------- ------ ---------");
        httpSinkConfig = ConfigFactory.create(HTTPSinkConfig.class, config);
        statsDClient = new NonBlockingStatsDClient(getPrefix(appConfig.getDataDogPrefix()), appConfig.getDataDogHost(),
                appConfig.getDataDogPort(), appConfig.getDataDogTags().split(","));
        clockInstance = new Clock();
    }

    public LogConsumer buildConsumer() {
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
            sink = HttpSink();
        } else {
            sink = LogSink();
        }
        return new LogConsumer(consumer, sink, statsDClient, clockInstance);
    }

    private Sink HttpSink() {
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(httpSinkConfig.getRequestTimeoutInMs())
                .setConnectionRequestTimeout(httpSinkConfig.getRequestTimeoutInMs())
                .setConnectTimeout(httpSinkConfig.getRequestTimeoutInMs())
                .build();
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(httpSinkConfig.getMaxHttpConnections());
        CloseableHttpClient closeableHttpClient = HttpClients.custom().setConnectionManager(connectionManager).setDefaultRequestConfig(requestConfig).build();
        BaseHttpClient client = new ExponentialBackoffClient(closeableHttpClient, statsDClient, clockInstance, httpSinkConfig);

        GenericHTTPClient httpClient = new GenericHTTPClient(appConfig.getServiceURL(),
                Header.parse(appConfig.getHTTPHeaders()), client);
        return new HttpSink(httpClient);
    }

    private static String getPrefix(String prefix) {
        return "log.consumer." + prefix;
    }

    private Sink LogSink() {
        LogConfig logConfig = ConfigFactory.create(LogConfig.class, config);
        logger.info("--------- LogSink ---------");
        logger.info(logConfig.getProtoSchema());
        logger.info("--------- ------- ---------");
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
