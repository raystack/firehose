package io.odpf.firehose.config.converter;

import io.odpf.firehose.config.enums.FilterDataSource;
import org.aeonbits.owner.Converter;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Method;

public class FilterTypeConverter implements Converter<FilterDataSource> {
    @Override
    public FilterDataSource convert(Method method, String input) {
        if (StringUtils.isNotEmpty(input)) {
            return FilterDataSource.valueOf(input.toUpperCase());
        } else {
            return FilterDataSource.NONE;
        }
    }
}
