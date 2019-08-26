package com.gojek.esb.config.converter;

import com.gojek.esb.config.enums.ESMessageType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class ESMessageTypeConverter implements Converter<ESMessageType> {
    @Override
    public ESMessageType convert(Method method, String input) {
        return ESMessageType.valueOf(input.toUpperCase());
    }
}
