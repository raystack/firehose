package io.odpf.firehose.config.converter;

import io.odpf.firehose.sink.file.writer.PartitioningType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class FileSinkPartitioningTypeConverter implements Converter<PartitioningType> {
    @Override
    public PartitioningType convert(Method method, String input) {
        return PartitioningType.valueOf(input.toUpperCase());
    }
}
