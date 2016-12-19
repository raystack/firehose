package com.gojek.esb.factory;

import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.consumer.EsbGenericConsumer;
import com.gojek.esb.consumer.LogConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

public class LogConsumerFactory {

    private static final Logger logger = LoggerFactory.getLogger(LogConsumerFactory.class.getName());

    private static final KafkaConsumerConfig kafkaConsumerConfig = new KafkaConsumerConfig(FactoryUtils.appConfig.getKafkaAddress(),
            FactoryUtils.appConfig.getConsumerGroupId(),
            Pattern.compile(FactoryUtils.appConfig.getKafkaTopic()),
            Long.MAX_VALUE,
            System.getenv()
    );

    private static final EsbGenericConsumer genericConsumer = new GenericKafkaFactory().createConsumer(kafkaConsumerConfig);

    public static LogConsumer getLogConsumer() {
        return new LogConsumer(genericConsumer, FactoryUtils.httpClient, FactoryUtils.statsDClient, FactoryUtils.clockInstance);
    }
}
