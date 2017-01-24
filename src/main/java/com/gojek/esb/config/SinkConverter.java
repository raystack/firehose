package com.gojek.esb.config;

import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class SinkConverter implements Converter<SinkType> {
    @Override
    public SinkType convert(Method method, String input) {
        return SinkType.valueOf(input.toUpperCase());
    }
}
