package com.gojek.esb.launch;

import com.gojek.esb.client.GenericHTTPClient;
import com.gojek.esb.config.ApplicationConfiguration;
import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.consumer.EsbGenericConsumer;
import com.gojek.esb.consumer.LogConsumer;
import com.gojek.esb.factory.GenericKafkaFactory;
import org.aeonbits.owner.ConfigFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;


public class Main {
    public static void main(String[] args) throws IOException {
        ApplicationConfiguration appConfig = ConfigFactory.create(ApplicationConfiguration.class, System.getenv());

        KafkaConsumerConfig kafkaConsumerConfig = new KafkaConsumerConfig(appConfig.getKafkaAddress(),
                appConfig.getConsumerGroupId(),
                Pattern.compile(appConfig.getKafkaTopic()),
                Long.MAX_VALUE
        );

        EsbGenericConsumer genericConsumer = new GenericKafkaFactory().createConsumer(kafkaConsumerConfig);
        GenericHTTPClient client = new GenericHTTPClient(appConfig.getServiceURL(), parseHeaders(appConfig.getHTTPHeaders()));

        LogConsumer logConsumer = new LogConsumer(genericConsumer, client);

        while (true) {
            logConsumer.processPartitions();
        }
    }

    private static Map<String, String> parseHeaders(String headers) {
        HashMap<String, String> map = new HashMap<>();

        String[] headerStrings = headers.split(",");
        for (String string : headerStrings) {
            if (!string.trim().isEmpty()) {
                String[] keyValue = string.trim().split(":");
                map.put(keyValue[0], keyValue[1]);
            }
        }

        return map;
    }
}
