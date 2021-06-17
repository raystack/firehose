package io.odpf.firehose.config.converter;

import io.odpf.firehose.sink.objectstorage.Constants;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class ObjectStorageTypeConverter implements Converter<Constants.ObjectStorageType> {
    @Override
    public Constants.ObjectStorageType convert(Method method, String input) {
        return Constants.ObjectStorageType.valueOf(input.toUpperCase());
    }
}
