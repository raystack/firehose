package io.odpf.firehose.config.converter;

import io.odpf.firehose.sink.cloud.Constants;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class CloudSinkWriterTypeConverter implements Converter<Constants.WriterType> {
    @Override
    public Constants.WriterType convert(Method method, String input) {
        return Constants.WriterType.valueOf(input.toUpperCase());
    }
}
