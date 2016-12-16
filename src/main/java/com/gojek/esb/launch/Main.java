package com.gojek.esb.launch;

import com.gojek.esb.client.GenericHTTPClient;
import com.gojek.esb.config.ApplicationConfiguration;
import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.consumer.EsbGenericConsumer;
import com.gojek.esb.consumer.LogConsumer;
import com.gojek.esb.factory.GenericKafkaFactory;
import com.gojek.esb.parser.Header;
import com.gojek.esb.util.Clock;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import org.aeonbits.owner.ConfigFactory;

import java.io.IOException;
import java.util.regex.Pattern;


public class Main {
    public static void main(String[] args) throws IOException {
        ApplicationConfiguration appConfig = ConfigFactory.create(ApplicationConfiguration.class, System.getenv());

        KafkaConsumerConfig kafkaConsumerConfig = new KafkaConsumerConfig(appConfig.getKafkaAddress(),
                appConfig.getConsumerGroupId(),
                Pattern.compile(appConfig.getKafkaTopic()),
                Long.MAX_VALUE,
                System.getenv()
        );

        StatsDClient statsDClient = new NonBlockingStatsDClient(getPrefix(appConfig.getDataDogPrefix()), appConfig.getDataDogHost(),
                appConfig.getDataDogPort(), appConfig.getDataDogTags().split(","));

        EsbGenericConsumer genericConsumer = new GenericKafkaFactory().createConsumer(kafkaConsumerConfig);
        Clock clockInstance = new Clock();

        GenericHTTPClient client = new GenericHTTPClient(appConfig.getServiceURL(),
                Header.parse(appConfig.getHTTPHeaders()),statsDClient, clockInstance);

        LogConsumer logConsumer = new LogConsumer(genericConsumer, client, statsDClient, clockInstance);

        while (true) {
            logConsumer.processPartitions();
        }
    }

    private static String getPrefix(String prefix) {
        return "log.consumer." + prefix;
    }
}
