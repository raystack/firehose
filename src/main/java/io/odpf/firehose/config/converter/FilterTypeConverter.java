package io.odpf.firehose.config.converter;

import io.odpf.firehose.config.enums.FilterType;
import org.aeonbits.owner.Converter;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Method;

public class FilterTypeConverter implements Converter<FilterType> {
    @Override
    public FilterType convert(Method method, String input) {
        if (StringUtils.isNotEmpty(input)) {
            return FilterType.valueOf(input.toUpperCase());
        } else {
            return FilterType.NONE;
        }
    }
}
