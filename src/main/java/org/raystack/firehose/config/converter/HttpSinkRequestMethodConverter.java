package org.raystack.firehose.config.converter;

import org.raystack.firehose.config.enums.HttpSinkRequestMethodType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;


public class HttpSinkRequestMethodConverter implements Converter<HttpSinkRequestMethodType> {
    @Override
    public HttpSinkRequestMethodType convert(Method method, String input) {
        return HttpSinkRequestMethodType.valueOf(input.toUpperCase());
    }
}
