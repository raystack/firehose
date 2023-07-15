package org.raystack.firehose.config.converter;

import org.raystack.firehose.config.enums.KafkaConsumerMode;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class ConsumerModeConverter implements Converter<KafkaConsumerMode> {
    @Override
    public KafkaConsumerMode convert(Method method, String input) {
        return KafkaConsumerMode.valueOf(input.toUpperCase());
    }
}
