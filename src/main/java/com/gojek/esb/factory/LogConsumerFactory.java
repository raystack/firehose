package com.gojek.esb.factory;

import com.gojek.esb.audit.AuditMessageBuilder;
import com.gojek.esb.config.ApplicationConfiguration;
import com.gojek.esb.config.AuditConfig;
import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.consumer.EsbGenericConsumer;
import com.gojek.esb.consumer.LogConsumer;
import com.gojek.esb.server.AuditServiceResponseHandler;
import com.gojek.esb.sink.HttpSink;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.util.TimeUtil;
import org.asynchttpclient.DefaultAsyncHttpClient;

import java.util.Optional;
import java.util.regex.Pattern;

public class LogConsumerFactory {

    private static final ApplicationConfiguration appConfig = FactoryUtils.appConfig;

    private static final KafkaConsumerConfig kafkaConsumerConfig = new KafkaConsumerConfig(appConfig.getKafkaAddress(),
            FactoryUtils.appConfig.getConsumerGroupId(),
            Pattern.compile(FactoryUtils.appConfig.getKafkaTopic()),
            Long.MAX_VALUE,
            System.getenv()
    );

    private static final AuditConfig auditConfig = new AuditConfig(new DefaultAsyncHttpClient(), kafkaConsumerConfig.getGroupId(),
            appConfig.getAuditServiceUrl(), appConfig.isAuditEnabled(), Optional.of(new AuditServiceResponseHandler()), Optional.of(new AuditMessageBuilder(new TimeUtil())));

    private static final EsbGenericConsumer genericConsumer = new GenericKafkaFactory().createConsumer(kafkaConsumerConfig, auditConfig);

    public static LogConsumer getLogConsumer() {
        HttpSink sink = new HttpSink(FactoryUtils.httpClient);
        return new LogConsumer(genericConsumer, sink, FactoryUtils.statsDClient, FactoryUtils.clockInstance);
    }

    public static LogConsumer getLogConsumer(Sink sink) {
        return new LogConsumer(genericConsumer, sink, FactoryUtils.statsDClient, FactoryUtils.clockInstance);
    }
}
