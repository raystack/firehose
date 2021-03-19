package io.odpf.firehose.config.converter;

import io.odpf.firehose.config.enums.RedisSinkDataType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class RedisSinkDataTypeConverter implements Converter<RedisSinkDataType> {
    @Override
    public RedisSinkDataType convert(Method method, String input) {
        return RedisSinkDataType.valueOf(input.toUpperCase());
    }
}
