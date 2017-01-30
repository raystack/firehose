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
import com.gojek.esb.sink.DBSink;
import com.gojek.esb.sink.HttpSink;
import com.gojek.esb.sink.Sink;
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
            DBConfig dbConfig = ConfigFactory.create(DBConfig.class, config);
            sink = new DBSink(dbConfig);

        } else {
            sink = new HttpSink(FactoryUtils.httpClient);
        }
        return new LogConsumer(consumer, sink, FactoryUtils.statsDClient, FactoryUtils.clockInstance);
    }
}
