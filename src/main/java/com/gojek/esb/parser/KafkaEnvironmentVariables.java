package com.gojek.esb.parser;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaEnvironmentVariables {

    private static final String KAFKA_PREFIX = "source.kafka.consumer.config.";

    public static Map<String, String> parse(Map<String, String> envVars) {
        if (envVars == null || envVars.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> kafkaEnvVars = envVars.entrySet()
                .stream()
                .filter(a -> a.getKey().toLowerCase().startsWith(KAFKA_PREFIX))
                .collect(Collectors.toMap(e -> parseVarName(e.getKey()), e -> e.getValue()));
        return kafkaEnvVars;
    }

    private static String parseVarName(String varName) {
        return varName.toLowerCase().replaceAll(KAFKA_PREFIX, "");
    }
}
