package io.odpf.firehose.config.converter;

import io.odpf.firehose.config.enums.MongoSinkMessageType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class MongoSinkMessageTypeConverter implements Converter<MongoSinkMessageType> {
    @Override
    public MongoSinkMessageType convert(Method method, String input) {
        return MongoSinkMessageType.valueOf(input.toUpperCase());
    }
}
