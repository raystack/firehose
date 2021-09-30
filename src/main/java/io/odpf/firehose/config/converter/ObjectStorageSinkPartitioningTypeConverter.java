package io.odpf.firehose.config.converter;

import io.odpf.firehose.sink.objectstorage.Constants;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class ObjectStorageSinkPartitioningTypeConverter implements Converter<Constants.FilePartitionType> {
    @Override
    public Constants.FilePartitionType convert(Method method, String input) {
        return Constants.FilePartitionType.valueOf(input.toUpperCase());
    }
}
