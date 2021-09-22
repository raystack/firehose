package io.odpf.firehose.config.converter;

import io.odpf.firehose.config.enums.FilterEngineType;
import org.aeonbits.owner.Converter;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Method;

public class FilterEngineTypeConverter implements Converter<FilterEngineType> {
    @Override
    public FilterEngineType convert(Method method, String input) {
        if (StringUtils.isNotEmpty(input)) {
            return FilterEngineType.valueOf(input.toUpperCase());
        } else {
            throw new IllegalArgumentException("FILTER_ENGINE must be JSON or JEXL");
        }
    }
}
