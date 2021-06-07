package io.odpf.firehose.config.converter;

import io.odpf.firehose.sink.cloud.Constants;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class CloudSinkPartitioningTypeConverter implements Converter<Constants.PartitioningType> {
    @Override
    public Constants.PartitioningType convert(Method method, String input) {
        return Constants.PartitioningType.valueOf(input.toUpperCase());
    }
}
