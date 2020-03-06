package com.gojek.esb.config.converter;

import com.gojek.esb.config.enums.RedisTTLType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class RedisTTLTypeConverter implements Converter<RedisTTLType> {
    @Override
    public RedisTTLType convert(Method method, String input) {
        return RedisTTLType.valueOf(input.toUpperCase());
    }
}
