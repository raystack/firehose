package org.raystack.firehose.config.converter;

import org.raystack.firehose.sink.blob.Constants;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class BlobSinkFilePartitionTypeConverter implements Converter<Constants.FilePartitionType> {
    @Override
    public Constants.FilePartitionType convert(Method method, String input) {
        return Constants.FilePartitionType.valueOf(input.toUpperCase());
    }
}
