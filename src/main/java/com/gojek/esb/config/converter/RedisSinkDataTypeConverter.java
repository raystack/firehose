package com.gojek.esb.config.converter;

import com.gojek.esb.config.enums.RedisSinkDataType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class RedisSinkDataTypeConverter implements Converter<RedisSinkDataType> {
    @Override
    public RedisSinkDataType convert(Method method, String input) {
        return RedisSinkDataType.valueOf(input.toUpperCase());
    }
}
