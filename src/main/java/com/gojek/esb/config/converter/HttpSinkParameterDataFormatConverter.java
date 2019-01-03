package com.gojek.esb.config.converter;

import com.gojek.esb.config.enums.HttpSinkDataFormat;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;


public class HttpSinkParameterDataFormatConverter implements Converter<HttpSinkDataFormat> {
    @Override
    public HttpSinkDataFormat convert(Method method, String input) {
        return HttpSinkDataFormat.valueOf(input.toUpperCase());
    }
}
