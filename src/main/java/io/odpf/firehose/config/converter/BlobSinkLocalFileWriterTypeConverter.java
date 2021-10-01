package io.odpf.firehose.config.converter;

import io.odpf.firehose.sink.blob.Constants;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class BlobSinkLocalFileWriterTypeConverter implements Converter<Constants.WriterType> {
    @Override
    public Constants.WriterType convert(Method method, String input) {
        return Constants.WriterType.valueOf(input.toUpperCase());
    }
}
