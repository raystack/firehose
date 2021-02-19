package com.gojek.esb.config.converter;

import com.gojek.esb.config.enums.HttpSinkDataFormatType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class HttpSinkDataFormatTypeConverter implements Converter<HttpSinkDataFormatType> {
    @Override
    public HttpSinkDataFormatType convert(Method method, String input) {
        return HttpSinkDataFormatType.valueOf(input.toUpperCase());
    }
}
