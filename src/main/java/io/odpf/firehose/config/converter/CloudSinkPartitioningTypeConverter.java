package io.odpf.firehose.config.converter;

import io.odpf.firehose.sink.cloud.writer.PartitioningType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class CloudSinkPartitioningTypeConverter implements Converter<PartitioningType> {
    @Override
    public PartitioningType convert(Method method, String input) {
        return PartitioningType.valueOf(input.toUpperCase());
    }
}
