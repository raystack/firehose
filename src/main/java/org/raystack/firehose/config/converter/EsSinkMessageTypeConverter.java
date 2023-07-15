package org.raystack.firehose.config.converter;

import org.raystack.firehose.config.enums.EsSinkMessageType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class EsSinkMessageTypeConverter implements Converter<EsSinkMessageType> {
    @Override
    public EsSinkMessageType convert(Method method, String input) {
        return EsSinkMessageType.valueOf(input.toUpperCase());
    }
}
