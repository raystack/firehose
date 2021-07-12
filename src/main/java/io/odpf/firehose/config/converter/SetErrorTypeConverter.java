package io.odpf.firehose.config.converter;

import io.odpf.firehose.consumer.ErrorType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class SetErrorTypeConverter implements Converter<Set<ErrorType>> {
    @Override
    public Set<ErrorType> convert(Method method, String input) {
        String[] errors = input.split(",");
        return Arrays.stream(errors).map(ErrorType::valueOf).collect(Collectors.toSet());
    }
}
