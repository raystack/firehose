package com.gojek.esb.factory;

import com.gojek.esb.audit.AuditMessageBuilder;
import com.gojek.esb.config.ApplicationConfiguration;
import com.gojek.esb.config.AuditConfig;
import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.consumer.EsbGenericConsumer;
import com.gojek.esb.consumer.LogConsumer;
import com.gojek.esb.server.AuditServiceResponseHandler;
import com.gojek.esb.util.TimeUtil;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        return new LogConsumer(genericConsumer, FactoryUtils.httpClient, FactoryUtils.statsDClient, FactoryUtils.clockInstance);
    }
}
