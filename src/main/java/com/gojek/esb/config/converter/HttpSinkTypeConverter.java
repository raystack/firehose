package com.gojek.esb.config.converter;

import com.gojek.esb.config.enums.HttpSinkType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class HttpSinkTypeConverter implements Converter<HttpSinkType> {
    @Override
    public HttpSinkType convert(Method method, String input) {
        return HttpSinkType.valueOf(input.toUpperCase());
    }
}
