package com.gojek.esb.config.converter;

import com.gojek.esb.config.enums.EsbFilterType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class ESBFilterTypeConverter implements Converter<EsbFilterType> {
    @Override
    public EsbFilterType convert(Method method, String input) {
        return EsbFilterType.valueOf(input.toUpperCase());
    }
}
