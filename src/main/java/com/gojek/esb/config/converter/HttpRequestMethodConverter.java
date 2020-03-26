package com.gojek.esb.config.converter;

import com.gojek.esb.config.enums.HttpRequestMethod;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;


public class HttpRequestMethodConverter implements Converter<HttpRequestMethod> {
    @Override
    public HttpRequestMethod convert(Method method, String input) {
        return HttpRequestMethod.valueOf(input.toUpperCase());
    }
}
