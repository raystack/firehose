package io.odpf.firehose.config.converter;

import io.odpf.firehose.config.enums.FilterMessageType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class FilterMessageTypeConverter implements Converter<FilterMessageType> {
    @Override
    public FilterMessageType convert(Method method, String input) {
        try {
            return FilterMessageType.valueOf(input.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("FILTER_INPUT_MESSAGE_TYPE must be JSON or PROTOBUF");
        }
    }
}
