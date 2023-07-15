package org.raystack.firehose.config.converter;

import org.raystack.firehose.sink.dlq.DLQWriterType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class DlqWriterTypeConverter implements Converter<DLQWriterType> {
    @Override
    public DLQWriterType convert(Method method, String input) {
        return DLQWriterType.valueOf(input.toUpperCase());
    }
}
