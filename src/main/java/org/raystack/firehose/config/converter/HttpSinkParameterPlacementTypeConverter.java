package org.raystack.firehose.config.converter;

import org.raystack.firehose.config.enums.HttpSinkParameterPlacementType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class HttpSinkParameterPlacementTypeConverter implements Converter<HttpSinkParameterPlacementType> {
    @Override
    public HttpSinkParameterPlacementType convert(Method method, String input) {
        return HttpSinkParameterPlacementType.valueOf(input.toUpperCase());
    }
}
