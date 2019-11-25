package com.gojek.esb.config.converter;

import com.gojek.esb.config.enums.RedisSinkType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class RedisSinkTypeConverter implements Converter<RedisSinkType> {
    @Override
    public RedisSinkType convert(Method method, String input) {
        return RedisSinkType.valueOf(input.toUpperCase());
    }
}
